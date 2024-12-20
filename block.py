import pickle, logging
import fsconfig
import xmlrpc.client, socket, time

#### BLOCK LAYER

# global TOTAL_NUM_BLOCKS, BLOCK_SIZE, INODE_SIZE, MAX_NUM_INODES, MAX_FILENAME, INODE_NUMBER_DIRENTRY_SIZE

class DiskBlocks():
    def __init__(self):

        # initialize clientID
        if fsconfig.CID >= 0 and fsconfig.CID < fsconfig.MAX_CLIENTS:
            self.clientID = fsconfig.CID
        else:
            print('Must specify valid cid')
            quit()

        # initialize XMLRPC client connection to raw block server
        if fsconfig.START_PORT:
            START_PORT = fsconfig.START_PORT
        else:
            print('Must specify port number')
            quit()

        if fsconfig.NUM_SERVERS:
            NUM_SERVERS = fsconfig.NUM_SERVERS
        else:
            print('Must specify number of servers')
            quit()

        self.block_server = {}
        for server_id in range(0, NUM_SERVERS):
            server_url = 'http://' + fsconfig.SERVER_ADDRESS + ':' + str(START_PORT + server_id)
            self.block_server[server_id] = xmlrpc.client.ServerProxy(server_url, use_builtin_types=True)
        socket.setdefaulttimeout(fsconfig.SOCKET_TIMEOUT)

    def Xor(self, A, B):
        result = bytearray()
        for i in range(0, fsconfig.BLOCK_SIZE):
            result.append(A[i] ^ B[i])
        return result

    ## Put: interface to write a raw block of data to the block indexed by block number
    ## Blocks are padded with zeroes up to BLOCK_SIZE


    ## RAID 5 PUT and GET Implementation

    def getServerBlockAndParity(self, block_number):
        datablock_per_stripe = fsconfig.NUM_SERVERS - 1 # Number of data blocks per stripe
        stripe_number = block_number // datablock_per_stripe  # Stripe number
        data_offset = block_number % datablock_per_stripe # Offset of data block within the stripe

        # Calculate parity server for this stripe
        parity_server = stripe_number % fsconfig.NUM_SERVERS

        # Calculate server for the datablock
        # Identify all disks involved in the stripe
        blocks_in_stripe = list(range(fsconfig.NUM_SERVERS))
        blocks_in_stripe.remove(parity_server)  # Exclude parity disk
        server_index = blocks_in_stripe[data_offset]

        logging.debug(f"block_number: {block_number}, server_index: {server_index}, server_block_index:{stripe_number}, parity_server: {parity_server}")
        return server_index, stripe_number, parity_server # server, block_index, parity server for this block

    def Recover(self, server, block_number):
        old_data = bytearray(fsconfig.BLOCK_SIZE)
        for sid in range(0, fsconfig.NUM_SERVERS):
            if sid != server:
                error, rdata = self.SingleGet(sid, block_number)
                if error == -1 or error == -2:
                    print("Block recover failed due to one other server block down or corrupted!")
                    quit()
                old_data = self.Xor(old_data, rdata)
        return old_data


    def Put(self, block_number, block_data):

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # ljust does the padding with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # Write block
            server_index, block_index, parity_server = self.getServerBlockAndParity(block_number)
            error, old_data = self.SingleGet(server_index, block_index)
            cblk = False

            if error == -1:
                print(f"SERVER_DISCONNECTED PUT {block_number}")
                ## Collect old data from all other disks
                old_data = self.Recover(server_index, block_index)
                
                ## Update only the parity_data
                error, parity_data = self.SingleGet(parity_server, block_index)
                parity_data = self.Xor(parity_data, old_data)
                parity_data = self.Xor(parity_data, putdata)
                error = self.SinglePut(parity_server, block_index, parity_data)
                return 0
            
            if error == -2:
                cblk = True
                print(f"CORRUPTED_BLOCK {block_number}")
                old_data = self.Recover(server_index, block_index)

            ## No server error, so update the block with new put_data
            error = self.SinglePut(server_index, block_index, putdata)
            if error == -1:
                print("Server shutdown between Get and Put!")
                quit()

            ## Update the parity_data, ignore if the parity server is crashed
            error, parity_data = self.SingleGet(parity_server, block_index)
            if error != -1:
                parity_data = self.Xor(parity_data, old_data)
                parity_data = self.Xor(parity_data, putdata)
                error = self.SinglePut(parity_server, block_index, parity_data)
            elif error == -1 and cblk:
                print(f"Corrupted block PUT and parity server update failed!")
                quit()

            return 0
        
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()

    def Get(self, block_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            
            server_index, block_index, parity_server = self.getServerBlockAndParity(block_number)
            error, data = self.SingleGet(server_index, block_index)

            if error == -1:
                print(f"SERVER_DISCONNECTED GET {block_number}")
                data = self.Recover(server_index, block_index)

            elif error == -2:
                print(f"CORRUPTED_BLOCK {block_number}")
                data = data = self.Recover(server_index, block_index)

            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()


    ## RAID 4 PUT and GET implementation
    def PutRaid4(self, block_number, block_data):

        logging.debug(
            'Put: block number ' + str(block_number) + ' len ' + str(len(block_data)) + '\n' + str(block_data.hex()))
        if len(block_data) > fsconfig.BLOCK_SIZE:
            logging.error('Put: Block larger than BLOCK_SIZE: ' + str(len(block_data)))
            quit()

        server_id = block_number % (fsconfig.NUM_SERVERS - 1)
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # ljust does the padding with zeros
            putdata = bytearray(block_data.ljust(fsconfig.BLOCK_SIZE, b'\x00'))
            # Write block
            sblock_number = block_number // (fsconfig.NUM_SERVERS - 1)
            error, data = self.SingleGet(server_id, sblock_number)

            if error == -1:
                print(f"SERVER_DISCONNECTED PUT {block_number}")
                data = self.Get(server_id, block_number)
                error, parity = self.SingleGet(fsconfig.NUM_SERVERS - 1, sblock_number)
                parity = self.Xor(parity, data)
                parity = self.Xor(parity, putdata)
                ret1 = self.SinglePut(fsconfig.NUM_SERVERS - 1, sblock_number, parity)

                if ret1 == -1 or error == -1:
                    print("Parity server down, when other server is also down !! QUIT()")
                    quit()
                return 0
                
            # Normal Put
            ret = self.SinglePut(server_id, sblock_number, putdata)
            if ret == -1:
                print("SERVER NOT DOWN AND DOWN !! QUIT()")
                quit()

            error, parity = self.SingleGet(fsconfig.NUM_SERVERS - 1, sblock_number)
            parity = self.Xor(parity, data)
            parity = self.Xor(parity, putdata)
            ret1 = self.SinglePut(fsconfig.NUM_SERVERS - 1, sblock_number, parity)
            if ret1 == -1 or error == -1:
                print("Only parity server down")
            return 0
        
        else:
            logging.error('Put: Block out of range: ' + str(block_number))
            quit()


    ## Get: interface to read a raw block of data from block indexed by block number
    ## Equivalent to the textbook's BLOCK_NUMBER_TO_BLOCK(b)

    def GetRaid4(self, block_number):

        logging.debug('Get: ' + str(block_number))
        if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
            # call Get() method on the server
            server_id = block_number % (fsconfig.NUM_SERVERS - 1)
            sblock_number = block_number // (fsconfig.NUM_SERVERS - 1)
            error, data = self.SingleGet(server_id, sblock_number)
            if error == -1:
                print(f"SERVER_DISCONNECTED GET {block_number}")
                # construct from parity and return
                data = bytearray(fsconfig.BLOCK_SIZE)
                for sid in range(0, fsconfig.NUM_SERVERS):
                    if sid != server_id:
                        error, rdata = self.SingleGet(sid, sblock_number)
                        if error == -1:
                            print("MULTIPLE SERVER FAILED!")
                            quit()
                        data = self.Xor(data, rdata)

            return bytearray(data)

        logging.error('DiskBlocks::Get: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        quit()

## RSM: read and set memory equivalent

    def RSM(self, block_number):
        # Always return success
        logging.debug('RSM: ' + str(block_number))
        return bytearray(fsconfig.BLOCK_SIZE)
    
        # if block_number in range(0, fsconfig.TOTAL_NUM_BLOCKS):
        #     rpcretry = True
        #     while rpcretry:
        #         rpcretry = False
        #         try:
        #             data = self.block_server.RSM(block_number)
        #         except socket.timeout:
        #             print("SERVER_TIMED_OUT")
        #             time.sleep(fsconfig.RETRY_INTERVAL)
        #             rpcretry = True

        #     return bytearray(data)

        # logging.error('RSM: Block number larger than TOTAL_NUM_BLOCKS: ' + str(block_number))
        # quit()

        ## Acquire and Release using a disk block lock

    def Acquire(self):
        logging.debug('Acquire')
        RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        lockvalue = self.RSM(RSM_BLOCK);
        logging.debug("RSM_BLOCK Lock value: " + str(lockvalue))
        while lockvalue[0] == 1:  # test just first byte of block to check if RSM_LOCKED
            logging.debug("Acquire: spinning...")
            lockvalue = self.RSM(RSM_BLOCK);
        return 0

    def Release(self):
        logging.debug('Release')
        return 0
        # RSM_BLOCK = fsconfig.TOTAL_NUM_BLOCKS - 1
        # # Put()s a zero-filled block to release lock
        # self.Put(RSM_BLOCK,bytearray(fsconfig.RSM_UNLOCKED.ljust(fsconfig.BLOCK_SIZE, b'\x00')))
        # return 0

    ## Serializes and saves the DiskBlocks block[] data structure to a "dump" file on your disk

    def DumpToDisk(self, filename):

        logging.info("DiskBlocks::DumpToDisk: Dumping pickled blocks to file " + filename)
        file = open(filename,'wb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)
        pickle.dump(file_system_constants, file)
        pickle.dump(self.block, file)

        file.close()

    def SingleGet(self, server_id, block_number):
        logging.debug(f"GET server_index: {server_id}, block_number: {block_number}")
        try:
            error, data = self.block_server[server_id].Get(block_number)
        except ConnectionRefusedError:
            logging.debug(f"GET server_index: {server_id} is down")
            return -1, bytearray(fsconfig.BLOCK_SIZE)
        logging.debug(f"Datablock: {str(data.hex())}")
        return error, data

    def SinglePut(self, server_id, block_number, putdata):
        logging.debug(f"PUT server_index: {server_id}, block_number: {block_number}, data: {str(putdata.hex())}")
        try:
            ret = self.block_server[server_id].Put(block_number, putdata)
        except ConnectionRefusedError:
            logging.debug(f"PUT server_index: {server_id} is down")
            ret = -1
        return ret
    
    def Repair(self, repair_server):
        total_server_blocks = fsconfig.TOTAL_NUM_BLOCKS // (fsconfig.NUM_SERVERS - 1)
        for block in range(0, total_server_blocks):
            putdata = self.Recover(repair_server, block)
            ret = self.SinglePut(repair_server, block, putdata)
            if ret == -1:
                print("Repair server is down!")
                return -1, "REPAIR_SERVER_IS_DOWN"
        
        return 0, "SUCCESS"

    ## Loads DiskBlocks block[] data structure from a "dump" file on your disk

    def LoadFromDump(self, filename):

        logging.info("DiskBlocks::LoadFromDump: Reading blocks from pickled file " + filename)
        file = open(filename,'rb')
        file_system_constants = "BS_" + str(fsconfig.BLOCK_SIZE) + "_NB_" + str(fsconfig.TOTAL_NUM_BLOCKS) + "_IS_" + str(fsconfig.INODE_SIZE) \
                            + "_MI_" + str(fsconfig.MAX_NUM_INODES) + "_MF_" + str(fsconfig.MAX_FILENAME) + "_IDS_" + str(fsconfig.INODE_NUMBER_DIRENTRY_SIZE)

        try:
            read_file_system_constants = pickle.load(file)
            if file_system_constants != read_file_system_constants:
                print('DiskBlocks::LoadFromDump Error: File System constants of File :' + read_file_system_constants + ' do not match with current file system constants :' + file_system_constants)
                return -1
            block = pickle.load(file)
            for i in range(0, fsconfig.TOTAL_NUM_BLOCKS):
                self.Put(i,block[i])
            return 0
        except TypeError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered type error ")
            return -1
        except EOFError:
            print("DiskBlocks::LoadFromDump: Error: File not in proper format, encountered EOFError error ")
            return -1
        finally:
            file.close()


## Prints to screen block contents, from min to max

    def PrintBlocks(self,tag,min,max):
        print ('#### Raw disk blocks: ' + tag)
        for i in range(min,max):
            print ('Block [' + str(i) + '] : ' + str((self.Get(i)).hex()))
