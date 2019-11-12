import sys

from tsdemux.demux import TsParser
from tsdemux.es import Es
from tsdemux.pes import PesReader


class DvbSubitleParser(PesReader):

    def __init__(self, pid: int, es: Es):
        super().__init__(pid, es)
        self.verbose_debug = True

    def on_pes_packet_complete(self):
        if len(self.sections) != 1:
            self.warning("expecting subtitle payload to contain only one clear section")
            return

        section = self.sections[0]
        if section.scrambling != 0:
            self.warning("expecting subtitle payload to be clear")
            return

        data = section.data
        data_len = len(data)

        self.verbose(f"got dvb packet {data[:32].hex()}... (len: {data_len})")

        if data_len < 3:
            self.error(f"too short dvb subtitle pes {data_len}")
            return

        if data[0] != 0x20:
            self.warning(f"expecting data_identifier to be 0x20 got 0x{data[0]:02x}")
            return

        if data[1] != 0x00:
            self.warning(f"expecting subtitle_stream_id to be 0x00 got 0x{data[1]:02x}")
            return

        offset = 2
        data_len -= 2

        while data[offset] == 0x0F:
            # decode subtitle segment
            if data_len < 6:
                self.warning(f"segment is too short: {data_len}")
                break
            segment_type = data[offset + 1]
            page_id = data[offset + 2] << 8 | data[offset + 3]
            segment_len = data[offset + 4] << 8 | data[offset + 5]
            offset += 6
            data_len -= 6

            if segment_len > data_len:
                self.warning(f"out of bound segment: {segment_len} vs {data_len}")
                break

            # TODO: process segment
            self.verbose(f" - segment: {segment_type} | page: {page_id} | len: {segment_len}")

            offset += segment_len
            data_len -= segment_len

        if data[offset] != 0xFF:
            self.warning(f"expecting eof pes data marker, got 0x{data[offset]:02x}")

        if data_len != 1:
            self.warning(f"data left after processing: {data_len - 1}")

        sys.exit(0)


class DvbSubtitleExtractor(TsParser):
    def on_stream_added(self, program_id: int, pid: int, es: Es):
        if es.media_type != Es.MEDIA_TYPE_SUBTITLE:
            return
        if es.priv_stream_type == Es.DESCRIPTOR_TAG_DVB_SUBTITLE:
            self.info(f"found dvb subtitle with pid: 0x{pid:04x}")
            self.pid_handlers[pid] = DvbSubitleParser(pid, es)


if __name__ == '__main__':
    parser = DvbSubtitleExtractor()
    with open('sample.ts', 'rb') as tsfile:
        parser.parse(tsfile)
