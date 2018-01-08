import logging

from PyPPSPP import main

logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(asctime)s %(message)s')

class objectview(object):
    def __init__(self, d):
        self.__dict__ = d

def run():
    defaults = {}
    defaults['tracker'] = "127.0.0.1"
    defaults['filename'] = r"C:\PyPPSPP\vod.dat"
    defaults['swarmid'] = "513908d8bf82eb6aa254ea2b19e98d9df3b70d08"
    defaults['filesize'] = 10000000
    defaults['live'] = False
    defaults['livesrc'] = False
    defaults['tcp'] = True
    defaults['ledbat'] = True
    defaults['discardwnd'] = 1000
    defaults['alto'] = False
    defaults['skip'] = False
    defaults['buffsz'] = 500
    defaults['dlfwd'] = 0
    defaults['vod'] = False
    defaults['numpeers'] = None
    defaults['identifier'] = 'Test123'
    defaults['workdir'] = None
    defaults['output_dir'] = r'C:/PyPPSPP/'
    defaults['result_id'] = 'Develop1'

    main(objectview(defaults))

if __name__ == '__main__':
    run()
    