from real_migrate_to_neo4j import *
from os import system

def generate_csv():

	#read last block
	#file = open("graph_sync/last_block","r")

	last_block = 0
	#file.close()

	next_block = 0
	final_block = None

	#create 4 files with headers
	file_names = ["transactions.csv","addresses.csv","outputs.csv","inputs.csv"]
	headers = ["tx_hash,timestamp","wallet_address","tx_hash,wallet_address,amount","wallet_address,tx_hash,amount"]

	for (file_name,header) in zip(file_names,headers):
		file = open(file_name,"w")
		file.write(header+"\n")
		file.close()

	#generate CSVs
	raw_ordered_transactions_to_csv_main(next_block,final_block,file_names[0])
	raw_ordered_addresses_to_csv_main(next_block,final_block,file_names[1])
	raw_ordered_outputs_to_csv_main(next_block,final_block,file_names[2])
	populate_utxo_main(next_block,final_block)
	raw_ordered_inputs_to_csv_main(next_block,final_block,file_names[3])

	index_path = '/home/teh_devs/Downloads/bitcoin_analysis_sa/index'

	blockchain = Blockchain(os.path.expanduser('~/.bitcoin/blocks'))
	
	for block in blockchain.get_ordered_blocks(os.path.expanduser(index_path),start=last_block,end=final_block):
		#last_block = block.height

	#file = open("graph_sync/last_block","w")
	#file.write(str(last_block))
	#file.close()

if(__name__ == "__main__"):
	generate_csv()
	
	
	
	'''blockchain = Blockchain(os.path.expanduser('~/.bitcoin/blocks'))
	i = 0
	for block in blockchain.get_ordered_blocks(os.path.expanduser('~/Downloads/bitcoin_analysis_sa/index'),start=601765,end=None):
	i = block.height
	print(i)'''