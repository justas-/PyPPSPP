"""
Copyright 2017, J. Poderys, Technical University of Denmark

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Helper class to track data that is inflight and hide data structure.
"""
import collections

class InflightTrack(object):
    """In-Flight data tracker"""

    def __init__(self):
        """Initialize data structures"""
        self._deq = collections.deque() # Contains only seq numbers
        self._store = {}                # Contains [timestamp_sent, resent, data]

    def add(self, seq, time_stamp, data):
        """Add item to the list of data in-flight"""

        self._deq.appendleft(seq)
        self._store[seq] = [time_stamp, False, data]

    def peek(self):
        """Get the seq number of the right-most item"""
        return self._deq[-1]

    def pop(self, get_item=True):
        """Remove the rightmost item"""
        seq_num = self._deq.pop()

        if get_item:
            item = self._store[seq_num]
            del self._store[seq_num]
            return item
        else:
            del self._store[seq_num]

    def get_item(self, seq_num):
        """Get the indicated item"""
        return self._store[seq_num]

    def set_resent(self, seq_num):
        """Set given item as resent"""
        self._store[seq_num][1] = True

    def get_resendable(self, last_seq_acked):
        """Going right-to-left get all seq nums until last_seq_acked"""

        resendable = []
        for seq in reversed(self._deq):
            if last_seq_acked > seq:
                resendable.append(seq)

        return resendable

    def get_in_flight(self):
        """
        Get sequence number of all packets in-flight
        """
        in_flight = list(self._deq)
        return in_flight

    def pop_given(self, seq, return_item=True):
        """Remove diven SEQ number"""
        is_ooo = False

        # Check if there is at least on non-resent item
        for sent_seq in reversed(self._deq):
            # Exit as-soon as we find ourselves
            if sent_seq == seq:
                break

            (_, resent, _) = self._store[sent_seq]
            if resent:
                continue
            else:
                is_ooo = True
                break

        self._deq.remove(seq)
        (time_stamp, resent, data) = self._store[seq]
        del self._store[seq]

        if return_item:
            return (time_stamp, resent, data, is_ooo)

    def size(self):
        """Get size of deque"""
        return len(self._deq)
