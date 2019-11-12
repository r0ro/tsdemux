from typing import Callable

from tsdemux.es import Es
from tsdemux.psi import PsiTableReader


class PmtTableReader(PsiTableReader):

    def __init__(self, pid, program_id,
                 on_pcr_pid_changed: Callable[[int, int], None],
                 on_stream_added: Callable[[int, int, Es], None],
                 on_stream_removed: Callable[[int, int, Es], None]):
        super().__init__(pid, PsiTableReader.TABLE_ID_PMT)
        self.log_prefix = f"[PMT:0x{self.pid:04x}] "
        self.program_id = program_id
        self.prev_streams = {}
        self.streams = {}
        self.pcr_pid = -1
        self.on_pcr_pid_changed = on_pcr_pid_changed
        self.on_stream_added = on_stream_added
        self.on_stream_removed = on_stream_removed

    def check_section_headers(self, table_id: int, section_length: int, ext_id: int) -> bool:
        # additional checks
        if ((section_length >> 10) & 0x3) != 0:
            self.error(f"section length first 2 bits should be 0 got {section_length}")
            return False

        if section_length > 1021:
            self.error(f"section length is too long {section_length}")
            return False

        if self.program_id != ext_id:
            self.error(f"program_id mismatch {ext_id} vs {self.program_id}")
            return False

        return True

    def on_new_version(self, version: int):
        self.streams = {}

    def on_section(self, section_id: int, data: bytearray, crc32: int) -> bool:
        offset = 0
        data_len = len(data)

        if section_id != 0:
            self.warning(f"section id should be 0, got {section_id}")
            return False

        self.verbose(f"section {section_id} len: {data_len}")
        if data_len < 4:
            self.error(f"section length should not be < 4 bytes, got {data_len}")
            return False

        pcr_pid = ((data[offset] & 0x1F) << 8) | (data[offset + 1] & 0xFF)
        if pcr_pid != self.pcr_pid:
            self.info(f"pcr pid is now: {pcr_pid}")
            self.pcr_pid = pcr_pid
            self.on_pcr_pid_changed(self.program_id, self.pcr_pid)

        offset += 2
        data_len -= 2

        program_info_len = ((data[offset] & 0x0F) << 8) | (data[offset + 1] & 0xFF)
        offset += 2
        data_len -= 2

        if (program_info_len >> 10) != 0:
            self.error("first two bits of program info len should be 0")
            return False

        if program_info_len > data_len:
            self.error(f"info len out of bounds {program_info_len} vs {data_len}")
            return False

        self.verbose(f"program_info_len: {program_info_len}")
        # read program info
        if program_info_len > 0:
            # TODO: parse
            offset += program_info_len
            data_len -= program_info_len

        # read program entries
        while data_len >= 5:
            stream_type = data[offset] & 0xFF
            offset += 1

            es_pid = ((data[offset] & 0x1F) << 8) | (data[offset + 1] & 0xFF)
            offset += 2

            info_len = ((data[offset] & 0x0F) << 8) | (data[offset + 1] & 0xFF)
            offset += 2
            data_len -= 5

            if (info_len >> 10) != 0:
                self.error("first two bits of entry info len should be 0")
                return False

            if info_len > data_len:
                self.error(f"info_len out of bounds {info_len} vs {data_len}")
                return False

            self.streams[es_pid] = Es(es_pid, stream_type, data[offset:offset+info_len])

            if info_len > 0:
                offset += info_len
                data_len -= info_len

        if data_len:
            self.warning(f"left after reading {data_len}")

        return True

    def on_table_complete(self):
        self.info(f"============= PMT ({self.current_version}) =============")
        for pid, es in self.streams.items():
            self.info(f"> pid 0x{pid:04x} ==> {es}")
        self.info("===================================")

        for pid in self.streams.keys() - self.prev_streams.keys():
            self.info(f'  [+] pid: 0x{pid:04x} : {self.streams[pid]}')
            self.on_stream_added(self.program_id, pid, self.streams[pid])

        for pid in self.prev_streams.keys() - self.streams.keys():
            self.info(f'  [-] pid: 0x{pid:04x} : {self.prev_streams[pid]}')
            self.on_stream_removed(self.program_id, pid, self.prev_streams[pid])

        for pid in self.streams.keys() & self.prev_streams.keys():
            prev_es = self.prev_streams[pid]
            es = self.streams[pid]
            if prev_es != es:
                self.info(f'  [U] pid: 0x{pid:04x} => es changed from {prev_es} to {es}')
                self.on_stream_removed(self.program_id, pid, self.prev_streams[pid])
                self.on_stream_added(self.program_id, pid, self.streams[pid])

        self.prev_streams = self.streams.copy()

