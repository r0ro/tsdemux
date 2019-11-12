from tsdemux.logger import LogEnabled


class Es(LogEnabled):
    DESCRIPTOR_TAG_VIDEO = 0x02
    DESCRIPTOR_TAG_AUDIO = 0x03
    DESCRIPTOR_TAG_DATA_STREAM_ALIGNMENT = 0x06
    DESCRIPTOR_TAG_CA = 0x09
    DESCRIPTOR_TAG_LANGUAGE = 0x0A
    DESCRIPTOR_TAG_SERVICE = 0x48
    DESCRIPTOR_TAG_STREAM_IDENTIFIER = 0x52
    DESCRIPTOR_TAG_TELETEXT = 0x56
    DESCRIPTOR_TAG_DVB_SUBTITLE = 0x59
    DESCRIPTOR_TAG_AC_3 = 0x6a
    DESCRIPTOR_TAG_ENHANCED_AC_3 = 0x7a
    DESCRIPTOR_TAG_DTS = 0x7b
    DESCRIPTOR_TAG_EXTENDED = 0x7f
    DESCRIPTOR_TAG_SCTE35_CUE = 0x8a

    MEDIA_TYPE_UNKNOWN = -1
    MEDIA_TYPE_VIDEO = 0
    MEDIA_TYPE_AUDIO = 1
    MEDIA_TYPE_SUBTITLE = 2

    # Stream Types
    # See http://www.atsc.org/cms/standards/Code-Points-Registry-Rev-35.xlsx
    STREAM_TYPE_MPEG1_VIDEO = 0x01
    STREAM_TYPE_MPEG2_VIDEO = 0x02
    STREAM_TYPE_MPEG1_AUDIO = 0x03
    STREAM_TYPE_MPEG2_AUDIO = 0x04
    STREAM_TYPE_PRIVATE = 0x06
    STREAM_TYPE_AUDIO_ADTS = 0x0f
    STREAM_TYPE_H264 = 0x1b
    STREAM_TYPE_MPEG4_VIDEO = 0x10
    STREAM_TYPE_METADATA = 0x15
    STREAM_TYPE_AAC = 0x11
    STREAM_TYPE_MPEG2_VIDEO_2 = 0x80
    STREAM_TYPE_AC3 = 0x81
    STREAM_TYPE_PCM = 0x83
    STREAM_TYPE_SCTE35 = 0x86

    STREAM_TYPES = {
        STREAM_TYPE_MPEG1_VIDEO: ('MPEG1 video', MEDIA_TYPE_VIDEO),
        STREAM_TYPE_MPEG2_VIDEO: ('MPEG2 video', MEDIA_TYPE_VIDEO),
        STREAM_TYPE_MPEG2_VIDEO_2: ('MPEG2 video (2)', MEDIA_TYPE_VIDEO),
        STREAM_TYPE_MPEG1_AUDIO: ('MPEG1 audio', MEDIA_TYPE_AUDIO),
        STREAM_TYPE_MPEG2_AUDIO: ('MPEG2 audio', MEDIA_TYPE_AUDIO),
        STREAM_TYPE_PRIVATE: ('Private stream', MEDIA_TYPE_UNKNOWN),
        STREAM_TYPE_AUDIO_ADTS: ('ADTS', MEDIA_TYPE_AUDIO),
        STREAM_TYPE_H264: ('H264', MEDIA_TYPE_VIDEO),
        STREAM_TYPE_MPEG4_VIDEO: ('MPEG4', MEDIA_TYPE_VIDEO),
        STREAM_TYPE_METADATA: ('Metadata', MEDIA_TYPE_UNKNOWN),
        STREAM_TYPE_AAC: ('AAC', MEDIA_TYPE_AUDIO),
        STREAM_TYPE_AC3: ('AC3', MEDIA_TYPE_AUDIO),
        STREAM_TYPE_PCM: ('PCM', MEDIA_TYPE_AUDIO),
        STREAM_TYPE_SCTE35: ('SCTE-35', MEDIA_TYPE_UNKNOWN),
    }

    LANG_NAMES = {
        "fra": "Français",
        "fre": "Français",
        "qaa": "Version originale",
        "qad": "Audio Description",
        "deu": "Allemand",
        "eng": "Anglais",
        "ger": "Allemand",
        "ita": "Italien",
        "por": "Portugais",
        "spa": "Espagnol",
    }

    class DescriptorParseException(Exception):
        pass

    class Descriptor:
        def __init__(self, es, tag, data, offset, cur_len):
            self.tag = tag
            self.data = data[offset:offset+cur_len]

        def __str__(self):
            return f"[UNKNOWN: 0x{self.tag:02x} ({self.tag}) len: {len(self.data)} 0x{self.data.hex()}"

    class TeletextDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)
            es.priv_stream_type = Es.DESCRIPTOR_TAG_TELETEXT
            es.media_type = Es.MEDIA_TYPE_SUBTITLE
            es.name = "[SRT] Teletext subtitle"
            if cur_len < 5:
                raise Es.DescriptorParseException(f"missing subtitle information: {cur_len}")

            left = cur_len
            off = offset
            self.langs = {}

            while left >= 5:
                lang = data[off:off + 3].decode('ascii')
                off += 3
                teletext_type = data[off] >> 3
                teletext_magazine_number = data[off] & 0x7
                off += 1
                teletext_page_number = data[off]
                off += 1
                left -= 5
                self.langs[lang] = {
                    "type": teletext_type,
                    "magazine_number": teletext_magazine_number,
                    "page_number": teletext_page_number,
                }
                es.name += " | " + Es.LANG_NAMES.get(lang, lang)

        def __str__(self):
            desc = "[Teletext Subtitle:"
            for lang, infos in self.langs.items():
                lang_str = Es.LANG_NAMES.get(lang, lang)
                desc += f"LANG {lang_str}, type: {infos['type']}, " \
                        f"magazine_number: {infos['magazine_number']}, " \
                        f"page_number: {infos['page_number']}"
            desc += "]"
            return desc

    class DvbSubtitleDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)
            es.priv_stream_type = Es.DESCRIPTOR_TAG_DVB_SUBTITLE
            es.media_type = Es.MEDIA_TYPE_SUBTITLE
            es.name = "[SRT] Dvb subtitle"
            if cur_len < 8:
                raise Es.DescriptorParseException(f"missing subtitle information: {cur_len}")

            left = cur_len
            off = offset
            self.langs = {}

            while left >= 8:
                lang = data[off:off + 3].decode('ascii')
                off += 3
                subtitling_type = data[off] & 0xFF
                off += 1
                composition_page_id = data[off] << 8 | data[off + 1]
                off += 2
                ancillary_page_id = data[off] << 8 | data[off + 1]
                off += 2
                left -= 8
                self.langs[lang] = {
                    "subtitling_type": subtitling_type,
                    "composition_page_id": composition_page_id,
                    "ancillary_page_id": ancillary_page_id,
                }
                es.name += " | " + Es.LANG_NAMES.get(lang, lang)

        def __str__(self):
            desc = "[DVB Subtitle:"
            for lang, infos in self.langs.items():
                lang_str = Es.LANG_NAMES.get(lang, lang)
                desc += f" LANG {lang_str}, type: {infos['subtitling_type']}, " \
                        f"composition_page_id: {infos['composition_page_id']}, " \
                        f"ancillary_page_id: {infos['ancillary_page_id']}"
            desc += "]"
            return desc

    class CaDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)

            if tag in es.descriptors:
                raise Es.DescriptorParseException("a ca descriptor is already present")

            if cur_len < 4:
                raise Es.DescriptorParseException(f"too short ca_descriptor: {cur_len}")

            self.ca_system_id = ((data[offset] & 0xFF) << 8) | (data[offset + 1] & 0xFF)
            self.ca_pid = (data[offset + 2] & 0x1F) | (data[offset + 3] & 0xFF)

        def __str__(self):
            return f"[CA: system {self.ca_system_id} | pid: 0x{self.ca_pid:04x}]"

    class LanguageDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)
            if cur_len != 4:
                raise Es.DescriptorParseException(f"unexpected language descriptor len: {cur_len}")
            self.lang = data[offset:offset + 3].decode('ascii')
            es.langs.append(self.lang)
            offset += 3
            self.audio_type = data[offset]
            es.name += " | " + es.LANG_NAMES.get(self.lang, self.lang)

        def __str__(self):
            return f"[LANG: {self.lang}, audio_type {self.audio_type}]"

    class StreamIdentifierDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)
            self.identifier = data[offset:offset + cur_len]

        def __str__(self):
            return f"[STREAM_ID: {self.identifier.hex()}"

    class PrivAudioDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)
            es.priv_stream_type = tag
            es.media_type = Es.MEDIA_TYPE_AUDIO
            es.name = f"[AUD] AC3 or DTS (0x{tag:02x})"

    class Scte35CueDescriptor(Descriptor):
        def __init__(self, es, tag, data, offset, cur_len):
            super().__init__(es, tag, data, offset, cur_len)
            self.cue_stream_type = data[offset]

        def __str__(self):
            return f"[SCTE35 cue type: {self.cue_stream_type}]"

    DESCRIPTOR_TAG_TO_CLASS = {
        DESCRIPTOR_TAG_CA: CaDescriptor,
        DESCRIPTOR_TAG_DVB_SUBTITLE: DvbSubtitleDescriptor,
        DESCRIPTOR_TAG_LANGUAGE: LanguageDescriptor,
        DESCRIPTOR_TAG_TELETEXT: TeletextDescriptor,
        DESCRIPTOR_TAG_STREAM_IDENTIFIER: StreamIdentifierDescriptor,
        DESCRIPTOR_TAG_AC_3: PrivAudioDescriptor,
        DESCRIPTOR_TAG_ENHANCED_AC_3: PrivAudioDescriptor,
        DESCRIPTOR_TAG_DTS: PrivAudioDescriptor,
        DESCRIPTOR_TAG_SCTE35_CUE: Scte35CueDescriptor,
    }

    def __init__(self, pid: int, stream_type: int, descriptors: bytearray):
        super().__init__()
        self.log_prefix = "[ES:%04d] " % pid
        self.pid = pid
        self.stream_type = stream_type
        self.media_type = self.MEDIA_TYPE_UNKNOWN
        self.descriptors = {}
        self.name = ""
        self.langs = []
        self.priv_stream_type = -1
        codec_name = ""

        if stream_type in self.STREAM_TYPES:
            codec_name, self.media_type = self.STREAM_TYPES[stream_type]

        if self.media_type == self.MEDIA_TYPE_AUDIO:
            self.name = f"[AUD] {codec_name}"
        elif self.media_type == self.MEDIA_TYPE_VIDEO:
            self.name = f"[VID] {codec_name}"
        elif self.media_type == self.MEDIA_TYPE_SUBTITLE:
            self.name = f"[SUB] {codec_name}"
        else:
            self.name = f"unknown (stream_type: {stream_type})"

        self.process_descriptor(descriptors)

    def parse_descriptor(self, tag, data, offset, cur_len):
        try:
            if tag in self.DESCRIPTOR_TAG_TO_CLASS:
                return self.DESCRIPTOR_TAG_TO_CLASS[tag](self, tag, data, offset, cur_len)
            else:
                return self.Descriptor(self, tag, data, offset, cur_len)
        except Es.DescriptorParseException as e:
            self.warning(f"failed to parse descriptor {e}")
            return None

    def process_descriptor(self, data: bytearray):
        desc_len = len(data)
        offset = 0
        if desc_len == 0:
            return

        self.verbose("descriptors: len: %d %s", desc_len, data.hex())

        while desc_len > 2:
            tag = data[offset]
            cur_len = data[offset+1]
            offset += 2

            self.verbose("descriptor: 0x%02x (%d) len: %d %s", tag, tag,
                         cur_len, data[offset:offset+cur_len].hex())

            descriptor = self.parse_descriptor(tag, data, offset, cur_len)
            if descriptor is not None:
                self.descriptors[tag] = descriptor
                self.info(f"  - {descriptor}")

            offset += cur_len
            desc_len -= 2 + cur_len

    def __str__(self):
        return "[ES:%d|0x%04x] (stream_type: 0x%02x) %s" % (self.pid, self.pid, self.stream_type, self.name)
