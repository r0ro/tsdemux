from abc import abstractmethod
from typing import Any

from tsdemux.es import Es
from tsdemux.logger import LogEnabled
from tsdemux.reader import TsReader


class PesReader(LogEnabled, TsReader):

    class Section:
        def __init__(self, data=None, scrambling: int = 0):
            if data is None:
                self.data = bytearray()
            else:
                self.data = data
            self.scrambling = scrambling

    def __init__(self, pid: int, es: Es):
        super().__init__()
        self.log_prefix = f"{es.name} "
        self.pid = pid
        self.es = es
        self.pes_packet_len = 0
        self.data_left = 0
        self.pts = -1
        self.dts = -1
        self.cur_section: Any[None, PesReader.Section] = None
        self.sections = None

    @abstractmethod
    def on_pes_packet_complete(self):
        """Process complete pes packet"""
        pass

    def process_pes_packet(self):
        if self.cur_section is None:
            return

        if len(self.cur_section.data) != 0:
            self.sections.append(self.cur_section)
            self.cur_section = None

        if len(self.sections) == 0:
            self.sections = None

        self.on_pes_packet_complete()
        self.cur_section = None
        self.sections = None

    def append_data(self, data: bytearray, scrambling: int):
        data_len = len(data)
        if data_len == 0:
            return

        if self.cur_section is None:
            self.warning(f"dropping data: {data_len}")
            return

        if self.pes_packet_len > 0 and self.data_left == 0:
            self.warning(f"want to add too much data: {data_len}")
            return

        if self.cur_section.scrambling != scrambling:
            self.verbose(f"need a new section scrambling {self.cur_section.scrambling} => {scrambling}")
            if len(self.cur_section.data) > 0:
                self.sections.append(self.cur_section)
            self.cur_section = self.Section(scrambling=scrambling)

        if self.pes_packet_len > 0 and data_len >= self.data_left:
            if data_len > self.data_left:
                self.warning(f"adding too much data: {data_len} vs {self.data_left}")
            self.cur_section.data += data
            self.data_left = 0
            self.process_pes_packet()
        else:
            self.cur_section.data += data
            self.data_left -= data_len

    @staticmethod
    def read_pts(data: bytearray, offset: int) -> float:
        """
        Read the 33bits timestamp and return convert to a timestamp value in ms
        """
        a = data[offset] & 0x0E
        b = data[offset + 1] & 0xFF
        c = data[offset + 2] & 0xFF
        d = data[offset + 3] & 0xFF
        e = data[offset + 4] & 0xFF

        return (((a & 0x0E) << 29) |
                (((((b << 8) | c) & 0xFFFF) >> 1) << 15) |
                ((((d << 8) | e) & 0xFFFF) >> 1)) / 90

    def read_payload(self, data: bytearray, pusi: bool, scrambling: int, discontinuity: bool):
        if not pusi:
            self.append_data(data, scrambling)
            return

        if self.sections is not None and self.pes_packet_len > 0 and self.data_left > 0:
            self.warning(f"missing end of pes packet: {self.data_left}")

        # process prev packet
        self.process_pes_packet()

        # check start code
        if data[0:3] != b'\x00\x00\x01':
            self.warning(f"bad start code 0x{data[0:3].hex()}")
            return

        # skip pes start code
        offset = 3
        data_len = len(data) - 3

        # process headers
        stream_id = data[offset] & 0xFF
        offset += 1
        packet_len = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF)
        offset += 2

        self.verbose(f"stream_id: {stream_id}, packet_len: {packet_len}")

        # new pes packet
        self.sections = []
        self.cur_section = self.Section(scrambling=scrambling)

        # skip some flags
        if (data[offset] & 0xC0) != 0x80:
            self.warning("invalid marker bytes")
        offset += 1

        has_pts = (data[offset] & 0x80) != 0
        has_dts = (data[offset] & 0x40) != 0
        offset += 1

        # read header length
        header_len = data[offset]
        offset += 1

        data_len -= 6 + header_len
        if packet_len > 0:
            packet_len -= 3 + header_len

        if has_pts:
            self.pts = self.read_pts(data, offset)
        else:
            self.pts = 0

        if has_dts:
            # cannot have dts without pts
            self.dts = self.read_pts(data, offset + 5)
        else:
            self.dts = 0

        self.data_left = packet_len

        self.verbose(f"[PES] packet len: {self.pes_packet_len} "
                     f"PTS: {self.pts}, DTS: {self.dts}, header len: {header_len}")
        # skip rest of header
        offset += header_len

        self.append_data(data[offset:offset+data_len], scrambling=scrambling)
