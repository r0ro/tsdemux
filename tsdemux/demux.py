#!/usr/bin/env python3

from typing import Dict

from tsdemux.es import Es
from tsdemux.logger import LogEnabled
from tsdemux.pat import PatTableReader
from tsdemux.pmt import PmtTableReader
from tsdemux.reader import TsReader


class TsParser(LogEnabled):

    TS_PKT_LEN = 188
    TS_SYNC_BYTE = 0x47
    PAT_PID = 0x0000

    def __init__(self, verbose=False):
        super().__init__(verbose=verbose)
        self.continuity_counters = {}
        self.pkt_count = 0
        self.program_pids = {}
        self.pcr_ms = 0
        self.pmts = {}
        self.corrupted_packets = 0
        self.pid_handlers: Dict[int, TsReader] = {
            self.PAT_PID: PatTableReader(self.PAT_PID, self.on_program_added, self.on_program_removed)
        }
        self.programs_pcr_pid = {}
        self.programs_pcr = {}

    def on_pcr_pid_changed(self, program_id: int, new_pid: int):
        self.programs_pcr_pid[program_id] = new_pid
        self.programs_pcr[program_id] = 0

    def on_stream_added(self, program_id: int, pid: int, es: Es):
        pass

    def on_stream_removed(self, program_id: int, pid: int, es: Es):
        pass

    def on_program_added(self, program_id, pid):
        self.pid_handlers[pid] = PmtTableReader(pid, program_id,
                                                self.on_pcr_pid_changed,
                                                self.on_stream_added,
                                                self.on_stream_removed)

    def on_program_removed(self, program_id, pid):
        if pid in self.pid_handlers:
            del self.pid_handlers[pid]
        if program_id in self.programs_pcr:
            del self.programs_pcr[program_id]

    def decode_adaptation_field(self, pid, data):
        offset = 0
        data_len = len(data)
        adaptation_field_len = data[offset] & 0xFF
        offset += 1
        if adaptation_field_len > data_len - 1:
            self.warning(f"adaptation field len truncated {adaptation_field_len} vs {data_len} (pid: {pid})")
            return

        if adaptation_field_len == 0:
            return

        flags = data[offset] & 0xFF
        offset += 1

        if flags & 0x80:
            self.verbose("discontinuity indicator")
        if flags & 0x40:
            self.verbose("random_access_indicator")
        if flags & 0x20:
            self.verbose("es_priority_indicator")
        pcr_present = flags & 0x10
        if pcr_present:
            pcr = (((data[offset] & 0xFF) << 25) |
                   ((data[offset + 1] & 0xFF) << 17) |
                   ((data[offset + 2] & 0xFF) << 9) |
                   ((data[offset + 3] & 0xFF) << 1) |
                   (((data[offset + 4] & 0xFF) >> 7) & 0x1)) / 90
            for program_id, pcr_pid in self.programs_pcr_pid.items():
                if pcr_pid == pid:
                    self.programs_pcr[program_id] = pcr
                    self.verbose(f"Program {program_id} pcr: {pcr}")

    def parse_pkt(self, data):
        offset = 1

        # read header
        transport_error_indicator = (data[offset] & (1 << 7)) != 0
        pusi = (data[offset] & (1 << 6)) != 0
        pid = ((data[offset] << 8) | (data[offset + 1] & 0xFF)) & 0x1FFF
        scrambled = (data[offset + 2] >> 6) & 0x3
        afield_ctrl = (data[offset + 2] >> 4) & 0x3
        continuity_counter = data[offset + 2] & 0xF
        offset += 3

        discontinuity = False

        if pid == 0x1FFF:
            # skip padding packet
            return

        if transport_error_indicator:
            # skip corrupted packet
            self.corrupted_packets += 1
            self.warning("transport_error_indicator")
            return

        self.verbose("TS PKT [%06d|pid:0x%04x%s]" % (self.pkt_count, pid, pusi and "|PUSI" or ""))

        # check if payload is present
        if (afield_ctrl & 0x1) == 0:
            # no payload is present
            self.decode_adaptation_field(pid, data[offset:])
            return

        if pid not in self.continuity_counters:
            self.continuity_counters[pid] = continuity_counter
        else:
            if self.continuity_counters[pid] == 15:
                self.continuity_counters[pid] = 0
            else:
                self.continuity_counters[pid] += 1

            if self.continuity_counters[pid] != continuity_counter:
                self.warning("continuity check failed for PID 0x%02x (%02d vd %02d)" % (
                    pid, continuity_counter, self.continuity_counters[pid]))
                discontinuity = True
                self.continuity_counters[pid] = continuity_counter

        # skip adaptation field if present
        if afield_ctrl & 0x2 != 0:
            afield_len = 0xFF & data[offset]
            offset += 1
            self.verbose(f"afield_len: {afield_len}")
            if afield_len > 183 or (afield_len == 183 and afield_ctrl != 0x2):
                self.error(f"invalid adaptation field length: {afield_len} @{self.pkt_count}")
                self.corrupted_packets += 1
                return

            self.decode_adaptation_field(pid, data[offset-1:offset+afield_len+1])

            # skip adaptation field
            offset += afield_len

        return pid, pusi, discontinuity, scrambled, data[offset:]

    def parse(self, stream):
        while True:
            ts_pkt = stream.read(self.TS_PKT_LEN)
            if not ts_pkt:
                break

            idx = 0
            while ts_pkt[idx] != self.TS_SYNC_BYTE:
                # resync
                self.warning("need resync: %02x vs %02x" % (ts_pkt[0], self.TS_SYNC_BYTE))
                extra = tsfile.read(1)
                if not extra:
                    break
                ts_pkt = ts_pkt[1:] + extra

            pid, pusi, discountinuity, scrambled, payload = self.parse_pkt(ts_pkt)

            if pid in self.pid_handlers:
                self.pid_handlers[pid].read_payload(payload, pusi, scrambled, discountinuity)

            self.pkt_count += 1

        self.info("done")


if __name__ == '__main__':
    parser = TsParser(verbose=False)
    with open('sample.ts', 'rb') as tsfile:
        parser.parse(tsfile)
