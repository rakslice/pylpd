import bisect
import os
import socket

import select

import shutil

import subprocess
import traceback

import time

FILE_DOWNLOAD_TIMEOUT = 20


def hexdump(s):
    return "".join("%02x " % ord(ch) for ch in s)


POST_CONVERT_SCRIPT_PATH = r"C:\Program Files (x86)\gs\gs9.15\bin"
POST_CONVERT_SCRIPT = r"C:\Program Files (x86)\gs\gs9.15\lib\ps2pdf.bat"


script_path = os.path.dirname(os.path.abspath(__file__))
queues_dir = os.path.join(script_path, "queues")


MODE_TOP = 1
MODE_RECEIVE_JOB_SUBCOMMANDS = 2
MODE_RECEIVE_FILE = 3


JOB_HAS_DATA = 1
JOB_HAS_CONTROL = 2


def contents(filename):
    with open(filename, "rb") as handle:
        return handle.read()


class Connection(object):
    def __init__(self, my_socket, address):
        """:type my_socket: socket.socket"""
        self.my_socket = my_socket
        self.peer_address = address
        self.current_queue_name = None

        self.current_command = ""

        self.mode = MODE_TOP

        self.current_file_handle = None
        self.bytes_left = None

        self.current_job_name = None

        self.job_state_flags = None

        self.deadline = None

    def consider_current_file_download_complete(self):
        self.bytes_left = None
        self.current_file_handle.close()
        self.mode = MODE_RECEIVE_JOB_SUBCOMMANDS

        if self.current_job_name[0] == "c":
            self.job_state_flags.add(JOB_HAS_CONTROL)
        elif self.current_job_name[0] == "d":
            self.job_state_flags.add(JOB_HAS_DATA)

        self.my_socket.send("\x00")  # ok

    def do_read(self):
        """:return: False if the connection is closed"""

        if self.mode == MODE_RECEIVE_FILE:
            assert self.bytes_left is not None
            assert self.current_file_handle is not None

            data_read = self.my_socket.recv(1024)
            if data_read == "":
                # connection closed
                print "Connection closed during file transfer"
                self.current_file_handle.close()
                self.current_file_handle = None
                self.bytes_left = None
                return False

            assert len(data_read) <= self.bytes_left
            self.current_file_handle.write(data_read)
            self.bytes_left -= len(data_read)
            print "%s %s bytes_left %s" % (self.current_queue_name, self.current_job_name, self.bytes_left)

            self.bump_deadline(FILE_DOWNLOAD_TIMEOUT)

            if self.bytes_left == 0:
                self.clear_deadline()
                self.consider_current_file_download_complete()
        else:
            data_read = self.my_socket.recv(1024)
            if data_read == "":

                if self.mode == MODE_RECEIVE_JOB_SUBCOMMANDS \
                  and JOB_HAS_CONTROL in self.job_state_flags \
                  and JOB_HAS_DATA in self.job_state_flags:
                    job_suffix = self.current_job_name[1:]
                    self.process_completed_job(self.current_queue_name, job_suffix)

                return False
            print self.peer_address
            print hexdump(data_read), repr(data_read)
            self.current_command += data_read

            if "\n" in self.current_command:
                pos = self.current_command.index("\n")
                command = self.current_command[:pos]
                self.current_command = self.current_command[pos + 1:]
                if self.mode == MODE_TOP:
                    command_result = self.run_command(command)
                else:
                    command_result = self.run_job_subcommand(command)
                if not command_result:
                    print "command indicated close"
                    return False

        return True

    def post_remote_close(self):
        pass

    def before_local_close(self):
        pass

    def do_read_exceptional(self):
        data_read = self.my_socket.recv(1, socket.MSG_OOB)
        print self.peer_address, "exception"
        print "ex byte: %s" % hexdump(data_read)

    def run_command(self, command):
        opcode = command[0]
        params = command[1:]
        if opcode == "\x02":
            # receive a printer job
            self.current_queue_name = params
            self.mode = MODE_RECEIVE_JOB_SUBCOMMANDS
            self.job_state_flags = set()
            self.bump_deadline(FILE_DOWNLOAD_TIMEOUT)
            self.my_socket.send("\x00")  # ok
        elif opcode == "\x03":
            queue_name, user_names_or_job_numbers_list_str = params.split(" ", 1)
            return self.describe_queues_short(queue_name, user_names_or_job_numbers_list_str)
        else:
            print "Unknown opcode %r" % opcode
            return False
        return True

    def describe_queues_short(self, queue_name, user_names_or_job_numbers_list_str):
        # Nothing yet
        return False

    def run_job_subcommand(self, command):
        opcode = command[0]
        params = command[1:]
        if opcode == "\x02" or opcode == "\x03":
            is_data_file = opcode == "\x03"  # otherwise control file
            count_str, job_name = params.split(" ", 1)
            count = int(count_str)

            queue_dir = os.path.join(queues_dir, self.current_queue_name)
            if not os.path.isdir(queue_dir):
                os.mkdir(queue_dir)

            current_filename = os.path.join(queue_dir, job_name)
            self.current_file_handle = open(current_filename, "wb")
            self.current_job_name = job_name

            self.bytes_left = count

            print "Queue %s receiving %s file %s (%d bytes)" % (self.current_queue_name,
                                                                "data" if is_data_file else "control",
                                                                self.current_job_name, self.bytes_left)

            self.mode = MODE_RECEIVE_FILE
            self.my_socket.send("\x00")  # ok
        else:
            print "Unknown job subcommand code %r" % opcode
            return False
        return True

    def process_completed_job(self, current_queue_name, job_suffix):
        control_filename = os.path.join(queues_dir, current_queue_name, "c" + job_suffix)
        control_contents = contents(control_filename)
        control_lines = control_contents.split("\n")

        job_real_name = [line[1:] for line in control_lines if line.startswith("N")][0]

        ext = ".ps"

        job_real_name = job_real_name.replace("/", "_")
        job_real_name = job_real_name.replace("\\", "_")
        job_real_name = job_real_name.replace(":", "_")

        extra_suffix = ""
        attempt = 1
        while True:
            output_filename = os.path.join(queues_dir, current_queue_name, job_real_name + extra_suffix + ext)
            if os.path.exists(output_filename):
                attempt += 1
                extra_suffix = " #%d" % attempt
            else:
                break

        for line in control_lines:
            if line == "":
                continue
            ch = line[0]
            param = line[1:]
            if ch == "l":
                data_filename_proper = param
                assert data_filename_proper == "d" + job_suffix
                data_filename = os.path.join(queues_dir, current_queue_name, data_filename_proper)
                shutil.copy(data_filename, output_filename)
            if ch == "U":
                data_filename_proper = param
                assert data_filename_proper == "d" + job_suffix
                data_filename = os.path.join(queues_dir, current_queue_name, data_filename_proper)
                os.remove(data_filename)

        os.remove(control_filename)

        self.done_with_queue_job(output_filename)

    # noinspection PyMethodMayBeStatic
    def done_with_queue_job(self, output_filename):
        print "Post-processing %r" % output_filename
        # noinspection PyBroadException
        try:
            env = dict(os.environ)
            env["PATH"] = POST_CONVERT_SCRIPT_PATH + ";" + env.get("PATH", "")
            subprocess.check_call([POST_CONVERT_SCRIPT, output_filename], env=env)
        except Exception:
            traceback.print_exc()
        print "Post-processing complete"

    def bump_deadline(self, timeout):
        self.deadline = time.time() + timeout

    def clear_deadline(self):
        self.deadline = None

    def fire_deadline(self):
        self.clear_deadline()
        if self.mode == MODE_RECEIVE_FILE:
            print "file transfer timed out"
            self.consider_current_file_download_complete()


def main():
    listening_socket = socket.socket()
    listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_OOBINLINE, 1)
    ip_address = "0.0.0.0"
    port = 515
    listening_socket.bind((ip_address, port))
    listening_socket.listen(1)
    print "Listening at %s:%d" % (ip_address, port)

    all_sockets = [listening_socket]
    connections_by_peername = {}
    """:type: dict[(str, int), Connection]"""

    while True:
        select_results = select.select(all_sockets, [], all_sockets, 2)
        """:type: (list[socket.socket], list[socket.socket], list[socket.socket])"""
        rlist, wlist, xlist = select_results

        if 0 == len(rlist) and 0 == len(wlist) and 0 == len(xlist):
            # timeout
            cur_time = time.time()
            for conn in connections_by_peername.values():
                if conn.deadline is not None and conn.deadline <= cur_time:
                    conn.fire_deadline()
            continue

        print [len(x) for x in (rlist, wlist, xlist)]

        for cur_socket in rlist:
            if cur_socket == listening_socket:
                new_socket, new_socket_address = listening_socket.accept()
                connection = Connection(new_socket, new_socket_address)
                print new_socket_address
                new_peername = new_socket.getpeername()
                print "peername is %r" % (new_peername,)
                connections_by_peername[new_peername] = connection
                all_sockets.append(new_socket)
            else:
                cur_peername = cur_socket.getpeername()
                connection = connections_by_peername[cur_peername]
                connection_still_open = connection.do_read()
                if not connection_still_open:
                    connection.post_remote_close()
                    del connections_by_peername[cur_peername]
                    all_sockets.remove(cur_socket)
                if cur_socket in xlist:
                    xlist.remove(cur_socket)

        for cur_socket in xlist:
            cur_peername = cur_socket.getpeername()
            print "socket in xlist %r, closing" % (cur_peername,)
            connection = connections_by_peername[cur_peername]
            connection.do_read_exceptional()
            # connection.before_local_close()
            # cur_socket.close()
            # del connections_by_peername[cur_peername]
            # all_sockets.remove(cur_socket)


if __name__ == "__main__":
    main()
