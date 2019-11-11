from abc import abstractmethod

from tsdemux.crc32 import Crc32
from tsdemux.logger import LogEnabled
from tsdemux.reader import TsReader


class PsiTableReader(LogEnabled, TsReader):

    TABLE_ID_PAT = 0x0
    TABLE_ID_CAT = 0x1
    TABLE_ID_PMT = 0x2

    # section data, max size defined by spec to 4096 for EIT,
    # 1024 for others, plus a full TS payload
    MAX_TABLE_SIZE = 4096 + 184

    def __init__(self, pid, table_id):
        super().__init__(log_name="psi", prefix="[PSI:%02d:%02d] " % (pid, table_id))
        self.pid = pid
        self.table_id = table_id
        self.current_version = -1
        self.last_section = -1
        self.sections_crc = {}
        self.table_complete = False
        self.payload_len = 0
        self.payload = None

    def reset(self):
        self.current_version = -1
        self.last_section = -1
        self.sections_crc.clear()
        self.table_complete = False
        self.payload_len = 0
        self.payload = None

    def handle_new_version(self, version):
        self.on_new_version(version)
        self.current_version = version
        self.last_section = -1
        self.sections_crc.clear()
        self.table_complete = False

    @abstractmethod
    def on_new_version(self, version: int):
        """Called when a new version of the table is received"""
        pass

    @abstractmethod
    def on_section(self, section_id: int, data: bytearray, crc32: int) -> bool:
        """
        Called when a new section has been received
        :returns True if section is valid
        """
        pass

    @abstractmethod
    def on_table_complete(self):
        """Called once all sections are available"""
        pass

    def check_section_headers(self, table_id: int, section_length: int, ext_id: int) -> bool:
        """
        Opportunity for subclasses to perform additional sanity checks on section header
        """
        return True

    def push_data(self, data: bytearray):
        """Accumulate data in section payload buffer"""
        if self.payload is None:
            self.warning("drop data, pusi not seen yet")
            return

        data_len = len(data)
        self.info(f"push_data {data.hex()}")

        if self.payload_len + data_len >= self.MAX_TABLE_SIZE:
            self.error("section length is too big ... dropping")
            self.reset()
            return

        self.payload += data
        self.payload_len += data_len

    def parse_section(self, offset: int, section_length: int) -> bool:
        start = offset
        data = self.payload[offset:offset+section_length+3]
        table_id = self.payload[offset] & 0xFF
        self.info(f"parse_section table_id: {table_id} {data.hex()}")
        section_syntax_indicator = self.payload[offset + 1] & 0x80 != 0
        private_indicator = self.payload[offset + 1] & 0x40 != 0
        crc_offset = offset + section_length - 4
        offset += 3

        if table_id != self.table_id:
            self.warning(f"unexpected table id {table_id} vs {self.table_id}")
            return True

        if not section_syntax_indicator:
            # private section without common syntax
            # TODO
            self.info("private section without common syntax")
            return True

        if private_indicator:
            # TODO
            self.info("private indicator")
            return True

        # parse common section fields
        ext_id = ((self.payload[offset] & 0xFF) << 8) | (self.payload[offset + 1] & 0xFF)
        offset += 2
        version = ((self.payload[offset] & 0xFF) >> 1) & 0x1F
        current = (self.payload[offset] & 0x1) != 0
        offset += 1
        cur_section = self.payload[offset] & 0xFF
        offset += 1
        last_section = self.payload[offset] & 0xFF
        offset += 1

        # check crc32
        crc = Crc32.compute(self.payload[start:start + section_length + 3])
        if crc != 0:
            self.error(f"invalid crc: got {crc}")
            return True

        if not self.check_section_headers(table_id, section_length, ext_id):
            self.error("invalid headers")
            return False

        crc32 = (((self.payload[crc_offset] & 0xFF) << 24)
                | ((self.payload[crc_offset + 1] & 0xFF) << 16)
                | ((self.payload[crc_offset + 2] & 0xFF) << 8)
                | (self.payload[crc_offset + 3] & 0xFF))

        # only consider current table
        if not current:
            return True

        if version != self.current_version:
            self.info(f"received a new version ({version} was {self.current_version}) of table {self.table_id}")
            self.handle_new_version(version)

        if cur_section > last_section:
            self.error(f"invalid current section {cur_section} (last: {last_section})")
            return True

        if last_section != self.last_section:
            if self.last_section >= 0:
                self.warning(f"unexpected last section number changed {self.last_section} => {last_section}")
                self.handle_new_version(version)
            self.last_section = last_section

        # check if we already have seen this section
        if cur_section in self.sections_crc:
            if self.sections_crc[cur_section] != crc32:
                self.warning(f"section {cur_section} crc changed without version change")
                self.handle_new_version(version)
            else:
                # same as before
                return True

        payload_length = section_length - 5 - 4

        self.info(f"received section {cur_section} / {last_section} of table_id {table_id}")

        if self.on_section(cur_section, self.payload[offset: offset+payload_length], crc32):
            self.sections_crc[cur_section] = crc32

        if not self.table_complete and len(self.sections_crc.keys()) == self.last_section + 1:
            self.info(f"table {table_id} is complete")
            self.on_table_complete()
            self.table_complete = True
        else:
            self.verbose(f"table not complete {len(self.sections_crc.keys())} / {self.last_section}")

        return True

    def parse_sections(self):
        left = self.payload_len
        offset = 0
        first = True

        self.info(f"parse_sections {self.payload.hex()}")

        while left > 3:
            if not first and self.payload[offset] & 0xFF == 0xFF:
                self.debug("discard padding after section: %d", left)
                left = 0
                break

            section_length = ((self.payload[offset + 1] << 8) | self.payload[offset + 2] & 0xFF) & 0xFFF

            # make sure we have a complete section
            if left - 3 < section_length:
                self.debug("section not complete (%d vs %d)", left - 3, section_length)
                break

            self.verbose(f"section_length {section_length}")

            if not self.parse_section(offset, section_length):
                break

            offset += section_length + 3
            left -= section_length + 3
            first = False

        if left > 0:
            self.debug(f"left after parse: {left}")

        if left > 0 and left != self.payload_len:
            self.payload = self.payload[offset:offset+left]
            self.payload_len = left

    def read_payload(self, data: bytearray, pusi: bool, scrambled: int, discontinuity: bool):

        if discontinuity:
            self.reset()

        if not pusi:
            # append data to previous
            self.push_data(data)

            # extract sections if complete
            self.parse_sections()
            return

        # read pointer field
        offset = 0
        data_len = len(data)
        pointer_field = data[offset]
        offset += 1

        self.verbose(f"pointer_field: {pointer_field}")

        if pointer_field >= data_len:
            self.error(f"pointer_field out of packet {pointer_field} vs {data_len}")
            self.reset()
            return

        if pointer_field > 0:
            # append reminder of data to prev payload
            self.push_data(data[offset:offset+pointer_field])

            offset += pointer_field
            data_len -= pointer_field

            # extract sections if complete
            self.parse_sections()

        # start new section
        self.payload_len = 0
        self.payload = bytearray()

        if (data[offset] & 0xFF) == 0xFF:
            self.warning("only padding found in table")
            return

        self.push_data(data[offset:offset+data_len])
        self.parse_sections()

