import asyncio
import csv

class ContentGenerator(object):
    """Create VP80 packets (talking head)"""

    def __init__(self, loop, callback, size_var):
        """Initialize the generator"""
        self._loop = loop
        self._callback = callback
        self._size_var = size_var

        self._video_samples = []
        self._audio_samples = []
        self._InitSamples()

        self._gen_handle = None
        self._fps = 10
        self._next_key = 0

    def StartGenerating(self):
        """Start the generator"""
        
        # Raise if already running
        if self._gen_handle != None:
            raise Exception

        # Send next and start scheduling
        self._GenNext()

    def StopGenerating(self):
        """Stop and reset the generator"""
        self._gen_handle.cancel()
        self._gen_handle = None

    def _GenNext(self):
        """Generate and schedule next output"""

        # Wrap
        if self._next_key == min([len(self._audio_samples), len(self._video_samples)]):
            self._next_key = 0

        # Get Sample
        video_sample = self._video_samples[self._next_key]
        audio_sample = self._audio_samples[self._next_key]

        # Save as object
        f = {}
        f['id'] = self._next_key # seq num

        int_k = int(video_sample['key'])        # Size of key frame
        int_n = int(video_sample['non-key'])    # Size of non-key frame

        if  int_n == 0:
            # Key frame has size -> key-frame
            f['vk'] = 1   # key-frame -> 1
            f['vd'] = int_k * bytes([192]) # video data
        else:
            f['vk'] = 0   # key-frame -> 1
            f['vd'] = int_n * bytes([192]) # video data

        f['ad'] = int(audio_sample['size']) * bytes([192]) # audio data

        # Send video frame
        self._callback(f)

        # Schedule next generation
        self._next_key += 1
        self._gen_handle = self._loop.call_later(1 / self._fps, self._GenNext)

    def _InitSamples(self):
        with open('CSV_Audio_Frames.csv', 'r') as audio_csv:
            ar = csv.DictReader(audio_csv)
            self._audio_samples = list(ar)

        with open('CSV_Video_Frames.csv', 'r') as video_csv:
            vr = csv.DictReader(video_csv)
            self._video_samples = list(vr)
