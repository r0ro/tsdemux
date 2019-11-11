from typing import Callable

from tsdemux.psi import PsiTableReader


class PatTableReader(PsiTableReader):

    def __init__(self,
                 pid,
                 on_program_added: Callable[[int, int], None],
                 on_program_removed: Callable[[int, int], None]):
        super().__init__(pid, PsiTableReader.TABLE_ID_PAT)
        self.log_prefix = f"[PAT:0x{self.pid:04x}] "
        self.prev_programs = {}
        self.programs = {}
        self.on_program_added = on_program_added
        self.on_program_removed = on_program_removed

    def check_section_headers(self, table_id: int, section_length: int, ext_id: int) -> bool:
        # additional checks
        if ((section_length >> 10) & 0x3) != 0:
            self.error(f"section length first 2 bits should be 0 got {section_length}")
            return False

        if section_length > 1021:
            self.error(f"section length is too long {section_length}")
            return False

        return True

    def on_new_version(self, version: int):
        self.programs = {}

    def on_section(self, section_id: int, data: bytearray, crc32: int) -> bool:
        data_len = len(data)
        offset = 0
        self.verbose(f"section {section_id} len: {data_len}")

        if data_len % 4:
            self.error(f"invalid section length {data_len}")
            return False

        while data_len >= 4:
            program_number = (data[offset] << 8 | (data[offset + 1] & 0xFF)) & 0xFFFF
            pid = ((data[offset + 2] << 8) | (data[offset + 3]) & 0xFF) & 0x1FFF
            if program_number == 0:
                self.info(f"> Network Id: {pid}")
            else:
                self.programs[program_number] = pid
            offset += 4
            data_len -= 4

        return True

    def on_table_complete(self):
        self.info(f"============= PAT ({self.current_version}) =============")
        for program, pid in self.programs.items():
            self.info(f"> program {program} ==> pid {pid} (0x{pid:04x})")
        self.info("===================================")

        for added_pgrm in self.programs.keys() - self.prev_programs.keys():
            self.info(f"  [+] [PROGRAM: {added_pgrm}] => pmt 0x{self.programs[added_pgrm]:04x}")
            self.on_program_added(added_pgrm, self.programs[added_pgrm])

        for removed_pgrm in self.prev_programs.keys() - self.programs.keys():
            self.info(f"  [-] [PROGRAM: {removed_pgrm}] => pmt 0x{self.programs[removed_pgrm]:04x}")
            self.on_program_removed(removed_pgrm, self.programs[removed_pgrm])

        for pgrm in self.programs.keys() & self.prev_programs.keys():
            prev_pid = self.prev_programs[pgrm]
            pid = self.programs[pgrm]
            if prev_pid != pid:
                self.info(f"  [U] [PROGRAM: {pgrm}] => pmt changed from 0x{prev_pid:04x} to 0x{pid:04x}")
                self.on_program_removed(pgrm, prev_pid)
                self.on_program_added(pgrm, pid)

        self.prev_programs = self.programs.copy()

