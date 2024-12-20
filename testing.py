def calculate_raid5_block(block_number, NUM_SERVERS):
    datablock_per_stripe = NUM_SERVERS - 1 # Number of data disks per stripe
    stripe_number = block_number // datablock_per_stripe  # Stripe number
    data_offset = block_number % datablock_per_stripe # Offset of data block within the stripe

    # Calculate parity server for this stripe
    parity_server = stripe_number % NUM_SERVERS

    # Calculate server for the datablock
    # Identify all disks involved in the stripe
    blocks_in_stripe = list(range(NUM_SERVERS))
    blocks_in_stripe.remove(parity_server)  # Exclude parity disk
    server_index = blocks_in_stripe[data_offset]
    return {
        "stripe_number": stripe_number,
        "parity_server": parity_server,
        "server_index": server_index,
    }

if __name__ == "__main__":
    num_servers = 4
    for vbn in range(45, 55):
        result = calculate_raid5_block(vbn, num_servers)

        print(f"Virtual Block Number: {vbn}")
        print(f"server_block_index: {result['stripe_number']}")
        print(f"parity_server: {result['parity_server']}")
        print(f"server_index: {result['server_index']}")
       
        print("*" * 30)
