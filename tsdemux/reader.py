from abc import abstractmethod


class TsReader:
    @abstractmethod
    def read_payload(self, data: bytearray, pusi: bool, scrambled: int, discontinuity: bool):
        pass
