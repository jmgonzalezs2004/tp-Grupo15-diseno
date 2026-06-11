import os
import struct
import logging

class WriteAheadLog:
    def __init__(self, cluster_config):
        self.config = cluster_config
        self.storage_dir = f"/data/wal_{cluster_config.cluster_name}_{cluster_config.node_id}"
        os.makedirs(self.storage_dir, exist_ok=True)
        self.filepath = os.path.join(self.storage_dir, "journal.bin")
        self.next_entry_id = 1
        self.pending_entries = {}
        self.fd = None
        self._load()

    def _load(self):
        if not os.path.exists(self.filepath):
            self.fd = open(self.filepath, "ab")
            return

        with open(self.filepath, "rb") as f:
            while True:
                header = f.read(9)
                if not header or len(header) < 9:
                    break
                
                entry_type, entry_id = struct.unpack(">BQ", header)
                if entry_type == 1:
                    length_data = f.read(4)
                    if not length_data or len(length_data) < 4:
                        break
                    length = struct.unpack(">I", length_data)[0]
                    data = f.read(length)
                    if len(data) < length:
                        break
                    self.pending_entries[entry_id] = data
                    if entry_id >= self.next_entry_id:
                        self.next_entry_id = entry_id + 1
                elif entry_type == 2:
                    if entry_id in self.pending_entries:
                        del self.pending_entries[entry_id]
        
        self.fd = open(self.filepath, "ab")
        self.compact()

    def write(self, raw_data: bytes) -> int:
        entry_id = self.next_entry_id
        self.next_entry_id += 1
        
        header = struct.pack(">BQI", 1, entry_id, len(raw_data))
        self.fd.write(header + raw_data)
        self.fd.flush()
        os.fsync(self.fd.fileno())
        
        self.pending_entries[entry_id] = raw_data
        return entry_id

    def mark_done(self, entry_id: int):
        if entry_id in self.pending_entries:
            data = struct.pack(">BQ", 2, entry_id)
            self.fd.write(data)
            self.fd.flush()
            os.fsync(self.fd.fileno())
            del self.pending_entries[entry_id]

    def recover(self) -> list:
        return list(self.pending_entries.values())

    def compact(self):
        if self.fd:
            self.fd.close()
        
        temp_filepath = self.filepath + ".tmp"
        with open(temp_filepath, "wb") as f:
            for entry_id, data in self.pending_entries.items():
                header = struct.pack(">BQI", 1, entry_id, len(data))
                f.write(header + data)
            f.flush()
            os.fsync(f.fileno())
        
        os.rename(temp_filepath, self.filepath)
        self.fd = open(self.filepath, "ab")
