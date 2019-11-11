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

    def __init__(self, pid: int, stream_type: int, descriptors: bytearray):
        super().__init__()
        self.log_prefix = "[ES:%04d] " % pid
        self.pid = pid
        self.stream_type = stream_type
        self.media_type = self.MEDIA_TYPE_UNKNOWN
        self.ca_pid = -1
        self.ca_system_id = -1
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

            if tag == self.DESCRIPTOR_TAG_CA:
                if cur_len < 4:
                    self.warning(f"too short ca_descriptor: {cur_len}")
                else:
                    ca_system_id = ((data[offset] & 0xFF) << 8) | (data[offset+1] & 0xFF)
                    self.verbose("ca_system_id: 0x%04x", ca_system_id)
                    ca_pid = (data[offset+2] & 0x1F) | (data[offset+3] & 0xFF)
                    self.verbose("ca_pid: %d (0x%04x)", ca_pid, ca_pid)
                    if self.ca_pid > 0:
                        self.error(f"es already has a ca pid defined: {self.ca_pid} vs {ca_pid}")
                    else:
                        self.ca_pid = ca_pid
                        self.ca_system_id = ca_system_id
            elif tag == self.DESCRIPTOR_TAG_LANGUAGE:
                if cur_len != 4:
                    self.warning(f"unexpected language descriptor length: {cur_len}")
                else:
                    self.langs = []
                    lang = data[offset:offset+3].decode('ascii')
                    self.langs.append(lang)
                    offset += 3
                    audio_type = data[offset]
                    self.name += " | " + self.LANG_NAMES.get(lang, lang)
                    self.verbose(f"name: {self.name} | audio type: {audio_type}")
            elif tag == self.DESCRIPTOR_TAG_STREAM_IDENTIFIER:
                self.info(f"stream identifier: {data[offset:offset+cur_len].hex()}")
            elif tag in (self.DESCRIPTOR_TAG_AC_3, self.DESCRIPTOR_TAG_ENHANCED_AC_3, self.DESCRIPTOR_TAG_DTS):
                self.priv_stream_type = tag
                self.media_type = self.MEDIA_TYPE_AUDIO
                self.name = "[AUD] AC3 or DTS (0x%02x)" % tag
            elif tag == self.DESCRIPTOR_TAG_TELETEXT:
                self.priv_stream_type = tag
                self.media_type = self.MEDIA_TYPE_SUBTITLE
                self.name = "[SRT] Teletext subtitle"
                if cur_len < 5:
                    self.warning("missing subtitle information")
                else:
                    left = cur_len
                    off = offset
                    self.langs = []
                    while left >= 5:
                        lang = data[off:off+3].decode('ascii')
                        self.langs.append(lang)
                        off += 3
                        teletext_type = data[off] >> 3
                        teletext_magazine_number = data[off] & 0x7
                        off += 1
                        teletext_page_number = data[off]
                        off += 1
                        left -= 5
                        self.name += " | " + self.LANG_NAMES.get(lang, lang)
                        self.verbose(f"lang: {lang}, type: {teletext_type}, "
                                     f"teletext_magazine_number: {teletext_magazine_number}, "
                                     f"teletext_page_number: {teletext_page_number}")
            elif tag == self.DESCRIPTOR_TAG_DVB_SUBTITLE:
                self.priv_stream_type = tag
                self.media_type = self.MEDIA_TYPE_SUBTITLE
                self.name = "[SRT] Dvb subtitle"
                if cur_len < 8:
                    self.warning("missing subtitle information")
                else:
                    left = cur_len
                    off = offset
                    self.langs = []
                    while left >= 8:
                        lang = data[off:off+3].decode('ascii')
                        self.langs.append(lang)
                        off += 3
                        subtitling_type = data[off] & 0xFF
                        off += 1
                        composition_page_id = data[off] << 8 | data[off + 1]
                        off += 2
                        ancillary_page_id = data[off] << 8 | data[off + 1]
                        off += 2
                        left -= 8
                        self.name += " | " + self.LANG_NAMES.get(lang, lang)
                        self.verbose(f"lang: {lang}, type: {subtitling_type}, "
                                     f"composition_page_id: {composition_page_id}, "
                                     f"ancillary_page_id: {ancillary_page_id}")
            elif tag == self.DESCRIPTOR_TAG_SCTE35_CUE:
                cue_stream_type = data[offset]
                self.verbose(f"SCTE 35 cue type: {cue_stream_type}")
            else:
                self.info("unknown descriptor: 0x%02x (%d) len: %d %s", tag, tag,
                          cur_len, data[offset:offset+cur_len].hex())

            offset += cur_len
            desc_len -= 2 + cur_len

    def __str__(self):
        desc = "[ES:%d|0x%04x] (stream_type: 0x%02x) %s" % (self.pid, self.pid, self.stream_type, self.name)
        if self.ca_pid > 0:
            desc += f" | ECM pid: {self.ca_pid}"
        return desc
