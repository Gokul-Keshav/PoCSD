import pickle, logging
import argparse
import time
import hashlib
import fsconfig

from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
  rpc_paths = ('/RPC2',)


def getChecksum(data):
    # Create an MD5 hash object
    md5_hash = hashlib.md5()
    # Update the hash object with the byte array
    md5_hash.update(data)
    # Return the raw 128-bit checksum as a byte array
    return md5_hash.digest()

class DiskBlocks():
  def __init__(self, total_num_blocks, block_size, delayat):
    # This class stores the raw block array
    self.block = []
    self.checksum = []
    # initialize request counter
    self.counter = 0
    self.getcounter = 0
    self.putcounter = 0
    self.delayat = delayat
    # Initialize raw blocks
    for i in range (0, total_num_blocks):
      putdata = bytearray(block_size)
      self.block.insert(i,putdata)
      self.checksum.insert(i, getChecksum(putdata))

  def Sleep(self):
    self.counter += 1
    if (self.counter % self.delayat) == 0:
      time.sleep(10)


if __name__ == "__main__":

  # Construct the argument parser
  ap = argparse.ArgumentParser()

  ap.add_argument('-nb', '--total_num_blocks', type=int, help='an integer value')
  ap.add_argument('-bs', '--block_size', type=int, help='an integer value')
  ap.add_argument('-port', '--port', type=int, help='an integer value')
  ap.add_argument('-delayat', '--delayat', type=int, help='an integer value')
  ap.add_argument('-cblk', '--corrupt_block', type=int, help="an integer value")

  args = ap.parse_args()

  if args.total_num_blocks:
    TOTAL_NUM_BLOCKS = args.total_num_blocks
  else:
    print('Must specify total number of blocks')
    quit()

  if args.block_size:
    BLOCK_SIZE = args.block_size
  else:
    print('Must specify block size')
    quit()

  if args.port:
    PORT = args.port
  else:
    print('Must specify port number')
    quit()

  if args.delayat:
    delayat = args.delayat
  else:
    # initialize delayat with artificially large number
    delayat = 1000000000

  if args.corrupt_block:
    cblk = args.corrupt_block
    print(f"Corrupt Block Configured: {cblk}")
  else:
    # initialize corrupt block with non existent block
    cblk = -1

  # initialize blocks
  RawBlocks = DiskBlocks(TOTAL_NUM_BLOCKS, BLOCK_SIZE, delayat)

  # Create server
  server = SimpleXMLRPCServer(("127.0.0.1", PORT), requestHandler=RequestHandler)

  def Get(block_number):
    RawBlocks.getcounter += 1
    print(f"GET BLOCK: {block_number}, getcounter: {RawBlocks.getcounter}")
    result = RawBlocks.block[block_number]
    RawBlocks.Sleep()
    if RawBlocks.checksum[block_number] != getChecksum(result):
      print("CHECKSUM ERROR RETURNED")
      return -2, result
    return 0, result

  server.register_function(Get)

  def Put(block_number, data):
    RawBlocks.putcounter += 1
    print(f"PUT BLOCK: {block_number}, putcounter: {RawBlocks.putcounter}")
    RawBlocks.block[block_number] = data.data
    RawBlocks.checksum[block_number] = getChecksum(data.data)
    if block_number == cblk:
      print("Check sum error emulated!")
      RawBlocks.block[block_number] = bytearray(BLOCK_SIZE)
    RawBlocks.Sleep()
    return 0

  server.register_function(Put)

  def RSM(block_number):
    RSM_LOCKED = bytearray(b'\x01') * 1
    result = RawBlocks.block[block_number]
    # RawBlocks.block[block_number] = RSM_LOCKED
    # RawBlocks.block[block_number] = bytearray(RSM_LOCKED.ljust(BLOCK_SIZE,b'\x01'))
    # RawBlocks.Sleep()
    return result

  server.register_function(RSM)

  # Run the server's main loop
  print ("Running block server with nb=" + str(TOTAL_NUM_BLOCKS) + ", bs=" + str(BLOCK_SIZE) + " on port " + str(PORT))
  server.serve_forever()