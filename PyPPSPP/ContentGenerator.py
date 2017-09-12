"""
PyPPSPP, a Python3 implementation of Peer-to-Peer Streaming Peer Protocol
Copyright (C) 2016,2017  J. Poderys, Technical University of Denmark

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""

"""
Generate content based on frame lengths given in CSV files.
"""
import asyncio
import csv

class ContentGenerator(object):
    """Create VP80 packets (talking head)"""

    def __init__(self):
        """Initialize the generator"""
        self._loop = asyncio.get_event_loop()

        self._video_samples = []
        self._audio_samples = []
        self._init_samples()

        self._gen_handle = None
        self._fps = 10
        self._next_key = 0

        self._gen_callbacks = []

    def add_on_generated_callback(self, callback):
        """Add callback that will be called when
           content is geenrated by the generator"""
        self._gen_callbacks.append(callback)

    def remove_on_generated_callback(self, callback):
        """Remove given callback from a list of
           callback that will be called after content
           is generated"""
        self._gen_callbacks.remove(callback)

    def _call_on_generated(self, data):
        """Call all callbacks with generated data"""
        for cb in self._gen_callbacks:
            cb(data)

    def start_generating(self):
        """Start the generator"""
        
        # Raise if already running
        if self._gen_handle != None:
            raise Exception

        # Send next and start scheduling
        self._gen_next()

    def stop_generating(self):
        """Stop and reset the generator"""
        self._gen_handle.cancel()
        self._gen_handle = None

    def _gen_next(self):
        """Generate and schedule next output"""

        # Wrap
        if self._next_key == min([len(self._audio_samples), len(self._video_samples)]):
            self._next_key = 0

        # Generate data
        avdata = self._get_next_avdata(self._next_key)

        # Send av_data
        self._call_on_generated(avdata)

        # Schedule next generation
        self._next_key += 1
        self._gen_handle = self._loop.call_later(1 / self._fps, self._gen_next)

    def _get_next_avdata(self, sameple_id):
        """Get next AV data piece"""

        # Get Sample
        video_sample = self._video_samples[sameple_id]
        audio_sample = self._audio_samples[sameple_id]

        # Save as object
        av_data = {}
        av_data['id'] = sameple_id              # seq num

        int_k = int(video_sample['key'])        # Size of key frame
        int_n = int(video_sample['non-key'])    # Size of non-key frame

        if  int_n == 0:
            # Key frame has size -> key-frame
            av_data['vk'] = 1   # key-frame -> 1
            av_data['vd'] = int_k * bytes([192]) # video data
        else:
            av_data['vk'] = 0   # key-frame -> 1
            av_data['vd'] = int_n * bytes([192]) # video data

        av_data['ad'] = int(audio_sample['size']) * bytes([192]) # audio data
        av_data['in'] = 'Quick Brow Fox!'

        return av_data

    def _init_samples(self):
        with open('CSV_Audio_Frames.csv', 'r') as audio_csv:
            ar = csv.DictReader(audio_csv)
            self._audio_samples = list(ar)

        with open('CSV_Video_Frames.csv', 'r') as video_csv:
            vr = csv.DictReader(video_csv)
            self._video_samples = list(vr)
