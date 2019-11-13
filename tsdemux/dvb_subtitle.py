import sys

from tsdemux.demux import TsParser
from tsdemux.es import Es
from tsdemux.pes import PesReader


class DvbSubtitlePage:

    class Region:
        def __init__(self, region_id, x, y):
            self.id = region_id
            self.x = x
            self.y = y
            self.fill = False
            self.width = -1
            self.height = -1
            self.level_of_compatibility = -1
            self.depth = -1
            self.clut_id = -1
            self.bg_color = -1
            self.objects = {}

        def __str__(self):
            return f"region {self.id} x: {self.x} y: {self.y} size: {self.width}x{self.height}"

    class Object:
        def __init__(self, object_id, object_type, x, y):
            self.id = object_id
            self.type = object_type
            self.x = x
            self.y = y
            self.foreground = -1
            self.background = -1

        def __str__(self):
            return f"object {self.id} x: {self.x} y: {self.y} type: {self.type}"

    def __init__(self, page_id):
        self.id = page_id
        self.display_width = 720
        self.display_height = 576
        self.window_width = self.display_width
        self.window_height = self.display_height
        self.window_x = 0
        self.window_y = 0
        self.timeout_seconds = -1
        self.regions = {}

    def __str__(self):
        return f"page {self.id} ({self.display_width}x{self.display_height})"


class DvbSubtitleParseError(Exception):
    pass


class DvbSubtitleParser(PesReader):

    SEGMENT_PAGE_COMPOSITION = 0x10
    SEGMENT_REGION_COMPOSITION = 0x11
    SEGMENT_CLUT_DEFINITION = 0x12
    SEGMENT_OBJECT_DATA = 0x13
    SEGMENT_DISPLAY_DEFINITION = 0x14
    SEGMENT_DISPARITY_SIGNALING = 0x15
    SEGMENT_ALTERNATIVE_CLUT = 0x16
    SEGMENT_END_OF_DISPLAY = 0x80

    SEGMENT_NAME = {
        SEGMENT_PAGE_COMPOSITION: "page_composition",
        SEGMENT_REGION_COMPOSITION: "region_composition",
        SEGMENT_CLUT_DEFINITION: "clut_definition",
        SEGMENT_OBJECT_DATA: "object_data",
        SEGMENT_DISPLAY_DEFINITION: "display_definition",
        SEGMENT_DISPARITY_SIGNALING: "disparity_signaling",
        SEGMENT_ALTERNATIVE_CLUT: "alternative_clut",
        SEGMENT_END_OF_DISPLAY: "end_of_display",
    }

    def __init__(self, pid: int, es: Es):
        super().__init__(pid, es)
        self.verbose_debug = True
        self.pages = {}
        dvb_subtitle_desc: Es.DvbSubtitleDescriptor = es.descriptors[Es.DESCRIPTOR_TAG_DVB_SUBTITLE]
        lang = list(dvb_subtitle_desc.langs.keys())[0]
        if len(dvb_subtitle_desc.langs.keys()) != 1:
            self.warning(f"only process first lang: {lang}")

        composition_page_id = dvb_subtitle_desc.langs[lang]['composition_page_id']
        ancillary_page_id = dvb_subtitle_desc.langs[lang]['ancillary_page_id']
        self.pages[composition_page_id] = DvbSubtitlePage(composition_page_id)
        if ancillary_page_id != composition_page_id:
            self.pages[ancillary_page_id] = DvbSubtitlePage(ancillary_page_id)

    def process_segment(self, segment_type: int, page: DvbSubtitlePage, data: bytearray):
        data_len = len(data)
        segment_name = self.SEGMENT_NAME.get(segment_type, "unknown")
        self.verbose(f"- segment: {segment_name} (0x{segment_type:02x}) | page {page} "
                     f"| len: {data_len} | data {data[:32].hex()}")

        if segment_type == self.SEGMENT_DISPLAY_DEFINITION:
            if data_len < 5:
                raise DvbSubtitleParseError(f"display definition segment is too short {data_len}")
            dds_version_number = data[0] >> 4
            display_window_flag = (data[0] & 0x08) != 0
            page.display_width = ((data[1] << 8) | data[2]) + 1
            page.display_height = ((data[3] << 8) | data[4]) + 1
            if display_window_flag:
                if data_len < 13:
                    raise DvbSubtitleParseError(f"display definition segment is too short {data_len}")
                x_min = (data[5] << 8) | data[6]
                x_max = (data[7] << 8) | data[8]
                y_min = (data[9] << 8) | data[10]
                y_max = (data[11] << 8) | data[12]
                page.window_x = x_min
                page.window_width = x_max - x_min
                page.window_y = y_min
                page.window_height = y_max - y_min
            else:
                page.window_width = page.display_width
                page.window_height = page.display_height
                page.window_x = 0
                page.window_y = 0
            self.verbose(f"    size: {page.display_width}x{page.display_height} "
                         f"window: x: {page.window_x}, y: {page.window_y}, "
                         f"size: {page.window_width}x{page.window_height}")
        elif segment_type == self.SEGMENT_PAGE_COMPOSITION:
            if data_len < 2:
                raise DvbSubtitleParseError(f"page composition segment is too short {data_len}")
            page.timeout_seconds = data[0]
            page_comp_version_number = data[1] >> 4
            page_state = (data[1] >> 2) & 0x03
            left = data_len - 2
            offset = 2
            self.verbose(f"    timeout: {page.timeout_seconds}, state: {page_state}")
            while left >= 6:
                region_id = data[offset]
                offset += 2
                region_x = (data[offset] << 8) | data[offset+1]
                offset += 2
                region_y = (data[offset] << 8) | data[offset + 1]
                offset += 2
                left -= 6
                if region_x >= page.display_width or region_y >= page.display_height:
                    raise DvbSubtitleParseError(f"region is out of bounds "
                                                f"{region_x}/{page.display_width} "
                                                f"{region_y}/{page.display_height}")
                region = page.regions.get(region_id)
                if region is None:
                    region = DvbSubtitlePage.Region(region_id, region_x, region_y)
                    page.regions[region_id] = region
                else:
                    region.x = region_x
                    region.y = region_y
                self.verbose(f"    {region}")
            if left != 0:
                self.warning(f"left over {left} when parsing page composition")
        elif segment_type == self.SEGMENT_REGION_COMPOSITION:
            if data_len < 10:
                raise DvbSubtitleParseError(f"region composition segment is too short {data_len}")
            region_id = data[0]
            region = page.regions.get(region_id)
            if region is None:
                region = DvbSubtitlePage.Region(region_id, 0, 0)
                page.regions[region_id] = region
            region_version_number = data[1] >> 4
            region.fill = (data[1] & 0x08) != 0
            region.width = (data[2] << 8) | data[3]
            region.height = (data[4] << 8) | data[5]
            region.level_of_compatibility = data[6] >> 5
            region.depth = 1 << ((data[6] >> 2) & 0x7)
            region.clut_id = data[7]

            if region.depth == 8:
                region.bg_color = data[8]
            elif region.depth == 4:
                region.bg_color = data[9] >> 4
            elif region.depth == 2:
                region.bg_color = data[9] & 0x3
            else:
                raise DvbSubtitleParseError(f"invalid region depth {region.depth}")
            self.verbose(f"  |- {region}")
            left = data_len - 10
            offset = 10
            while left >= 6:
                object_id = (data[offset] << 8) | data[offset+1]
                offset += 2
                object_type = data[offset] >> 6
                object_provider_flag = (data[offset] >> 4) & 0x3
                object_x = ((data[offset] & 0x0F) << 8) | data[offset+1]
                offset += 2
                object_y = ((data[offset] & 0x0F) << 8) | data[offset + 1]
                offset += 2
                left -= 6

                obj = region.objects.get(object_id)
                if obj is None:
                    obj = DvbSubtitlePage.Object(object_id, object_type, object_x, object_y)
                else:
                    obj.type = object_type
                    obj.x = object_x
                    obj.y = object_y

                if object_type == 1 or object_type == 2:
                    if left < 2:
                        raise DvbSubtitleParseError(f"region composition too short {left}")
                    obj.foreground = data[offset]
                    obj.background = data[offset+1]
                    offset += 2
                    left -= 2
                else:
                    obj.foreground = -1
                    obj.background = -1
                self.verbose(f"  |---- {obj}")
            if left != 0:
                self.warning(f"left over {left} when parsing region composition")



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

            if page_id in self.pages:
                self.process_segment(segment_type, self.pages[page_id], data[offset:offset+segment_len])

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
            self.pid_handlers[pid] = DvbSubtitleParser(pid, es)


if __name__ == '__main__':
    parser = DvbSubtitleExtractor()
    with open('sample.ts', 'rb') as tsfile:
        parser.parse(tsfile)
