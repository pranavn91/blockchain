import os
import subprocess
import traceback
from filter import Filter
from nx_classes import Address,Transaction
import networkx as nx
import networkx.drawing.nx_pydot as nx_draw
from queue import Queue
from blockchain_parser.blockchain import Blockchain
from neo4j import GraphDatabase,Session
from graphviz import Digraph
import json
from datetime import datetime
import pandas as pd
import time
import pickle
import csv
import plyvel
from multiprocessing import Process
import sys
import pymongo
import redis

address = "1EYSiRC2nUi2xLMTuwkWhHtpTTVVZ6KNrz"

def save_var(var,path =''):
    with open(path,'wb') as f:
        pickle.dump(var,f)

class TransactionDumpFilter(Filter):
	def process(self):
		count = 0
		blockchain = Blockchain(os.path.expanduser('~/.bitcoin/blocks'))
		for block in blockchain.get_unordered_blocks():
			timestamp = block.header.timestamp
			if(timestamp.year == 2017):
				for tx in block.transactions:
					self.output_queue.put((tx,block.header.timestamp))

		self.output_queue.put(None) #ending condition

class TransactionOrderedDumpFilter(Filter):

	def __init__(self,input_queue,output_queue,start_block,end_block):
		super().__init__(input_queue,output_queue)
		self.start_block = start_block
		self.end_block = end_block
	
	def process(self):
		count = 0
		blockchain = Blockchain(os.path.expanduser('~/.bitcoin/blocks'))
		for block in blockchain.get_ordered_blocks(os.path.expanduser('/home/teh_devs/Downloads/bitcoin_analysis_sa/index'),start=self.start_block,end=self.end_block):
			for tx in block.transactions:
				self.output_queue.put((tx,block.header.timestamp))
			
		self.output_queue.put(None) #ending condition

class ApiTransactionDumpFilter(Filter):
	def process(self):
		for i in range(0,52):
			file = open("bct/transactionGA/transac"+str(i)+".json","r")
			txs = json.loads(file.read())["txs"]

			count = 0
			for tx in txs:
				count += 1
				transaction = dict()
				ip = list()
				op = list()

				inputs = tx["inputs"]
				for inp in inputs:
					ip.append((inp["prev_out"]["addr"],inp["prev_out"]["value"]))

				for output in tx["out"]:
					op.append((output["addr"],output["value"]))

				transaction["inputs"] = ip
				transaction["outputs"] = op
				transaction["hash"] = tx["hash"]
				transaction["timestamp"] = tx["time"]
				self.output_queue.put(transaction)


		file.close()
		self.output_queue.put(None)
		
class ApiTransactionTimeDumpFilter(Filter):
	def process(self):
		timestamp = float('inf')
		for i in range(1,99):
			file = open("bct/wa/transacs"+str(i)+".json","r")
			txs = json.loads(file.read())["txs"]
			count = 0
			for tx in txs:
				count += 1
				transaction = dict()
				if (tx['time'] < timestamp):
					timestamp = tx['time']
		file.close()
		print(timestamp)



class TransactionReadFilter(Filter):
	def process(self):
		utxo = dict()
		while(True):

			next_element = self.input_queue.get(block=True)
			
			transaction = dict()
			ip = list()
			op = list()

			if(next_element is None):
				self.output_queue.put(None)
				return
			
			timestamp = next_element[1]
			next_element = next_element[0]
			
			tx_hash = next_element.hash
			utxo[tx_hash] = dict()

			if(next_element.is_coinbase()):
				output = next_element.outputs
				for index,output in enumerate(next_element.outputs):
					try:
						utxo[tx_hash][index] = (output.addresses[0].address,output.value)
						op.append((output.addresses[0].address,output.value))
					except:
						pass
			else:
				for index,output in enumerate(next_element.outputs):
					try:
						utxo[tx_hash][index] = (output.addresses[0].address,output.value)
						op.append((output.addresses[0].address,output.value))
					except:
						pass

				for inp in next_element.inputs:
					try:
						ip.append(utxo[inp.transaction_hash][inp.transaction_index])
						del utxo[inp.transaction_hash][inp.transaction_index]
					except:
						pass

			transaction["inputs"] = ip
			transaction["outputs"] = op
			transaction["hash"] = tx_hash
			transaction["timestamp"] = timestamp
			self.output_queue.put(transaction)


'''{
	"inputs":[(address,amount),(address,amount)],
	"outputs":[(address,amount),(address,amount)],
	"hash":txhash
}'''

class TransactionReadMongoFilter(Filter):
	def process(self):
		client = pymongo.MongoClient("mongodb://localhost:27017")
		utxo = client["bitcoin_analysis"]["utxo"]
		while(True):

			next_element = self.input_queue.get(block=True)

			if(next_element is None):
				self.output_queue.put(None)
				return
			
			timestamp = next_element[1]
			next_element = next_element[0]
			
			transaction = dict()
			
			ip = list()
			op = list()
			
			tx_hash = next_element.hash

			if(next_element.is_coinbase()):
				output = next_element.outputs
				output_sum = 0
				for index,output in enumerate(next_element.outputs):
					try:
						op.append((output.addresses[0].address,output.value))
						output_sum += output.value
					except:
						pass
				ip.append(("COINBASE",output_sum))
			else:
				for index,output in enumerate(next_element.outputs):
					try:
						op.append((output.addresses[0].address,output.value))
					except:
						pass

				for inp in next_element.inputs:
					try:
                        #main thing
						utxo_doc = utxo.find({"tx_hash":inp.transaction_hash,"index":inp.transaction_index})[0]
						ip.append((utxo_doc["address"],utxo_doc["amount"]))
						#flag in MongoDB
						
					except:
						pass

			transaction["inputs"] = ip
			transaction["outputs"] = op
			transaction["hash"] = tx_hash
			transaction["timestamp"] = timestamp
			self.output_queue.put(transaction)


class WriteUtxoFilter(Filter):
	def process(self):
		client = pymongo.MongoClient("mongodb://localhost:27017")
		utxo = client["bitcoin_analysis"]["utxo"]
		
		count = 0
		
		while(True):

			next_element = self.input_queue.get(block=True)

			if(next_element is None):
				return
			
			timestamp = next_element[1]
			next_element = next_element[0]
			
			tx_hash = next_element.hash

			output = next_element.outputs
			for index,output in enumerate(next_element.outputs):
				count += 1
				try:
					doc = {"tx_hash":tx_hash,"index":index,"address":output.addresses[0].address,"amount":output.value}
					utxo.insert(doc)
					
					if(count == 10000):
						#print(datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d"))
						count = 0
				except:
					pass
			

class ReadTransactionToCsvFilter(Filter):
	def __init__(self,input_queue,output_queue,file_name):
		super().__init__(input_queue,output_queue)
		
		self.file_name = file_name
	
	def process(self):
# 		utxo = dict()
		addresses = list()
		tx = list()
		ip_list = list()
		op_list = list()
		count = 0
		with open(self.file_name, 'a') as csvFile:
			writer = csv.writer(csvFile)
			while(True):
				if (count%50000 == 0):
					#print (count/1000)

					writer.writerows(tx)
					tx = []
				
				next_element = self.input_queue.get()

				if(next_element is None):
					self.output_queue.put(None)
					writer = csv.writer(csvFile)
					writer.writerows(tx)
					csvFile.close()

					return
				count +=1
				timestamp = int(datetime.timestamp(next_element[1]))
				next_element = next_element[0]
				tx_hash = next_element.hash
				record = [tx_hash,timestamp]
				tx.append(record)


'''{
	"inputs":[(address,amount),(address,amount)],
	"outputs":[(address,amount),(address,amount)],
	"hash":txhash
}'''

class ReadAddressToCsvFilter(Filter):
	def __init__(self,input_queue,output_queue,file_name):
		super().__init__(input_queue,output_queue)
		
		self.file_name = file_name
	
	def process(self):
# 		utxo = dict()
		addresses = list()
		tx = list()
		ip_list = list()
		op_list = list()
		count = 0
		with open(self.file_name, 'a') as csvFile:
			writer = csv.writer(csvFile)
			while(True):					
				if (count%5000 == 0):
					#print (count/1000)
					writer.writerows(addresses)
					addresses = []
				next_element = self.input_queue.get()

				if(next_element is None):
					self.output_queue.put(None)
					writer = csv.writer(csvFile)
					writer.writerows(addresses)
					csvFile.close()
					return
				
				next_element = next_element[0]
				output = next_element.outputs
				for index,output in enumerate(next_element.outputs):
					try:
						count +=1
						addresses.append([output.addresses[0].address])
						if (count % 100000 == 0):
							#print (count/1000)
							writer.writerows(addresses)
							addresses = []
					except:
						pass
		
						
						
class ReadOutputToCsvFilter(Filter):
	
	def __init__(self,input_queue,output_queue,file_name):
		super().__init__(input_queue,output_queue)
		
		self.file_name = file_name
	
	def process(self):
		op_list = list()
		count = 0
		with open(self.file_name, 'a') as csvFile:
			writer = csv.writer(csvFile)
			while(True):
				next_element = self.input_queue.get()

				if(next_element is None):
					#self.output_queue.put(None)
					writer.writerows(op_list)
					csvFile.close()
					return

				tx_hash = next_element["hash"]
				
				for index,output in enumerate(next_element["outputs"]):
					try:
						address = output[0]
						amount = output[1]
						op_list.append([tx_hash,address,amount])
						count += 1
						
						if (count % 10000 == 0):
							#print (count/1000)
							writer.writerows(op_list)
							op_list = []
						
					except:
						pass


class ReadInputToCsvFilter(Filter):
	
	def __init__(self,input_queue,output_queue,file_name):
		super().__init__(input_queue,output_queue)
		self.file_name = file_name
	
	def process(self):
		redis_client = redis.Redis(host="localhost",port=6379)
		client = pymongo.MongoClient("mongodb://localhost:27017")
		utxo = client["bitcoin_analysis"]["utxo"]
		
		ip_list = list()
		count = 0
		with open(self.file_name, 'a') as csvFile:
			writer = csv.writer(csvFile)
			while(True):
				next_element = self.input_queue.get()

				if(next_element is None):
					#self.output_queue.put(None)
					writer.writerows(ip_list)
					csvFile.close()
					return

				tx_hash = next_element["hash"]
				
				for index,inp in enumerate(next_element["inputs"]):
					try:
						address = inp[0]
						amount = inp[1]
						ip_list.append([address,tx_hash,amount])
						#update MongoDB
						query = {"tx_hash":tx_hash,"index":index}
						spent = {"$set":{"spent":True}}
						utxo.update(query,spent)
						count += 1
						
						if (count % 10000 == 0):
							print (count/1000)
							writer.writerows(ip_list)
							ip_list = []
						
					except:
						traceback.print_exc()

					
class NetworkXTransactionReadFilter(Filter):
	def process(self):
		G = nx.readwrite.gpickle.read_gpickle("graph.pickle")
		for node in G.nodes():
			if(isinstance(node,Transaction)):
				transaction = dict()
				ip = list()
				op = list()
				
				for (u,_,ddict) in G.in_edges(node,data=True):
					ip.append((u,ddict["amount"]))
					
				for(_,v,ddict) in G.out_edges(node,data=True):
					op.append((v,ddict["amount"]))
					
				transaction["inputs"] = ip
				transaction["outputs"] = op
				transaction["hash"] = node.tx_hash
				transaction["timestamp"] = node.timestamp
				
				self.output_queue.put(transaction)
				
		self.output_queue.put(None)
		
class EdgeCombineFilter(Filter):
	def process(self):
		while(True):
			next_element = self.input_queue.get()
			if(next_element is None):
				self.output_queue.put(None)
				return
			
			inputs = next_element["inputs"]
			
			contributions = dict()
			for inp in inputs:
				try:
					contributions[inp[0]] += inp[1]
				except:
					contributions[inp[0]] = inp[1]

			inputs = list()
			for (key,value) in contributions.items():
				inputs.append((key,value))
				
			outputs = next_element["outputs"]
			
			contributions = dict()
			for out in outputs:
				try:
					contributions[out[0]] += out[1]
				except:
					contributions[out[0]] = out[1]

			outputs = list()
			for (key,value) in contributions.items():
				outputs.append((key,value))

			next_element["inputs"] = inputs
			next_element["outputs"] = outputs
			
			self.output_queue.put(next_element)
			
class BtcUnitConvertFilter(Filter):
	def process(self):
		while(True):
			next_element = self.input_queue.get()
			if(next_element is None):
				self.output_queue.put(None)
				return
			
			inputs = list()
			for inp in next_element["inputs"]:
				inputs.append((inp[0],inp[1] / 100000000))
			
			outputs = list()
			for out in next_element["outputs"]:
				outputs.append((out[0],out[1] / 100000000))
			
			next_element["inputs"] = inputs
			next_element["outputs"] = outputs

			self.output_queue.put(next_element)
			
class GenerateCypherFilter(Filter):
	def process(self):
		while(True):

			next_element = self.input_queue.get()
			if(next_element is None):
				self.output_queue.put(None)
				return
			hash = next_element["hash"]
			timestamp = next_element["timestamp"]
			self.output_queue.put("create (t:Transaction{hash:'%s',timestamp:'%s'})" % (hash,timestamp))

			for inp in next_element["inputs"]:
				self.output_queue.put("merge (:Address{value:'%s'})" % (inp[0]))
				self.output_queue.put("match (t:Transaction),(a:Address) where t.hash='%s' and a.value='%s' create (a)-[:input{amount:%s}]-> (t)" % (hash,inp[0],inp[1]))

			for output in next_element["outputs"]:
				self.output_queue.put("merge (:Address{value:'%s'})" % (output[0]))
				self.output_queue.put("match (t:Transaction),(a:Address) where t.hash='%s' and a.value='%s' create (t)-[:output{amount:%s}]-> (a)" % (hash,output[0],output[1]))


class ExecuteCypherFilter(Filter):
	def process(self):
		driver = GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","bctvjti"))
		session = driver.session()

		print("Migrating...")
		k = 0
		while(True):
			next_element = self.input_queue.get()
			if(next_element is None):
				driver.close()
				return
			session.run(next_element)
			#print("Statement "+str(k),end="\r")
			k += 1
			
class QueryFileReadFilter(Filter):
	def process(self):
		directory = "query_files"
		no_of_files = len([name for name in os.listdir(directory) if os.path.isfile(name)])
		print(no_of_files)


class FileWriteFilter(Filter):
	def process(self):
		statement_count = 200000
		count = 0
		next_file_index = 0

		file = open("query_files/queries"+str(next_file_index)+".cypher","w")
		while(True):

			next_element = self.input_queue.get()
			if(next_element is None):
				return
		
			file.write(next_element+";")
			count += 1
			
			if(count % statement_count == 0):
				file.close()
				print(next_file_index,count)
				next_file_index += 1
				file = open("query_files/queries"+str(next_file_index)+".cypher","w")

class ConvertToNetworkXFormat(Filter):
	def process(self):
		self.G = nx.MultiDiGraph()
		addresses = set()
		while(True):

			next_element = self.input_queue.get()
			if(next_element is None):
				#write graph to file
				#nx.readwrite.gpickle.write_gpickle(G,"graph.pickle")
				return

			t = Transaction(next_element["hash"],next_element["timestamp"],None)
			for inp in next_element["inputs"]:
				self.G.add_edge(inp[0],t,type="input",amount=inp[1])

			for output in next_element["outputs"]:
				self.G.add_edge(t,output[0],type="output",amount=output[1])	


def testGenerateCypher():
	transaction = {
		"hash":"abcd",
		"inputs":[("abcd",10),("efgh",20)],
		"outputs":[("ijkl",10),("mnop",20)],
	}

	q1 = Queue()
	q2 = Queue()

	q1.put(transaction)
	q1.put(None)
	generate_cypher_filter = GenerateCypherFilter(q1,q2)

	generate_cypher_filter.start()
	generate_cypher_filter.join()

	while(not q2.empty()):
		print(q2.get())


def neo4j_main():

	q1 = Queue()
	q2 = Queue()
	q3 = Queue()

	transaction_dump_filter = TransactionDumpFilter(None,q1)
	transaction_read_filter = TransactionReadFilter(q1,q2)
	generate_cypher_filter = GenerateCypherFilter(q2,q3)
	execute_cypher_filter = ExecuteCypherFilter(q3,None)

	transaction_dump_filter.start()
	transaction_read_filter.start()
	generate_cypher_filter.start()
	execute_cypher_filter.start()

	execute_cypher_filter.join()

def networkx_main():

	q1 = Queue()

	api_transaction_dump_filter = ApiTransactionDumpFilter(None,q1)
	convert_to_networkx_format = ConvertToNetworkXFormat(q1,None)

	api_transaction_dump_filter.start()
	convert_to_networkx_format.start()
	
	convert_to_networkx_format.join()

					   
def api_dump_main():
	q1 = Queue()
	q2 = Queue()
					   
	api_transaction_dump_filter = ApiTransactionDumpFilter(None,q1)
	generate_cypher_filter = GenerateCypherFilter(q1,q2)
	execute_cypher_filter = ExecuteCypherFilter(q2,None)

	
	api_transaction_dump_filter.start()
	generate_cypher_filter.start()
	execute_cypher_filter.start()

	execute_cypher_filter.join()
	
def nx_to_neo4j_main():
	q1 = Queue()
	q2 = Queue()
	q3 = Queue()
	q4 = Queue()
	
	networkx_transaction_read_filter =  NetworkXTransactionReadFilter(None,q1)
	edge_combine_filter = EdgeCombineFilter(q1,q2)
	btc_unit_convert_filter = BtcUnitConvertFilter(q2,q3)
	generate_cypher_filter = GenerateCypherFilter(q3,q4)
	
	networkx_transaction_read_filter.start()
	edge_combine_filter.start()
	btc_unit_convert_filter.start()
	generate_cypher_filter.start()
	
	generate_cypher_filter.join()
	
	while(True):
		next_element = q4.get()
		if(next_element is None):
			return
		print(next_element+";")
		
def nx_to_nx():
	df = pd.read_csv("analysis/export.csv")
	
	q1 = Queue()
	q2 = Queue()
	q3 = Queue()
	
	networkx_transaction_read_filter =  NetworkXTransactionReadFilter(None,q1)
	edge_combine_filter = EdgeCombineFilter(q1,q2)
	btc_unit_convert_filter = BtcUnitConvertFilter(q2,q3)
	convert_to_nx_format_filter = ConvertToNetworkXFormat(q3,None)
	
	networkx_transaction_read_filter.start()
	edge_combine_filter.start()
	btc_unit_convert_filter.start()
	convert_to_nx_format_filter.start()
	
	convert_to_nx_format_filter.join()

	G = convert_to_nx_format_filter.G
	
	
	start_timestamp = 1434240000
	end_timestamp = 1511740800
	
	time_span = 604800 #one week
	
	for time in range(start_timestamp,end_timestamp,time_span):
		transactions = list()
		for node in G.nodes():
			if(isinstance(node,Transaction)):
				if(node.timestamp >= time and node.timestamp < time + time_span):
					transactions.append(node)
		
		if(len(transactions) == 0):
			continue
		
		start_date = datetime.fromtimestamp(time).strftime("%Y-%m-%d")
		end_date = datetime.fromtimestamp(time+time_span).strftime("%Y-%m-%d")
		image_name = start_date+" to "+end_date
		subG = Digraph(image_name,filename="images/"+image_name,format="png")
		subG.attr("node",shape="circle",style="filled,solid",fontsize="10",fontcolor="white",fontname="Arial",margin="0",penwidth="2")
		subG.attr("edge",fontsize="10")
		subG.attr("graph",label=image_name,labelloc="t")
		
		for tx in transactions:
			subG.node(tx.tx_hash,label=tx.tx_hash[:6]+"...",fillcolor="#57C7E3",color="#23B3D7")
			
			in_edges = G.in_edges(tx,data=True)
			out_edges = G.out_edges(tx,data=True)
			
			for (u,_,ddict) in in_edges:
				if(u == address):
					fill_color_value,color_value = "#F16667","#EB2728"
				else:
					fill_color_value,color_value = "#F79767","#F36924"
					
				subG.node(u,label=u[:6]+"...",fillcolor=fill_color_value,color=color_value)
				subG.edge(u,tx.tx_hash,label=str(ddict["amount"]),color="#BA311D")
				
			for (_,v,ddict) in out_edges:
				if(v == address):
					fill_color_value,color_value = "#F16667","#EB2728"
				else:
					fill_color_value,color_value = "#F79767","#F36924"
					
				subG.node(v,label=v[:6]+"...",fillcolor=fill_color_value,color=color_value)
				subG.edge(tx.tx_hash,v,label=str(ddict["amount"]),color="#569480")
				
		#subgraph generated
		#create dot language format
		subG.render()
		time_window_tx = df[(df["unix_timestamp"] >= int(time)) & (df["unix_timestamp"] < int(time + time_span))]
		time_window_tx.to_csv("images/"+image_name+".csv",index=False)
									  
def raw_ordered_to_cypher():
	q1 = Queue()
	q2 = Queue()
	q3 = Queue()
	q4 = Queue()
	q5 = Queue()
									  
	transaction_ordered_dump_filter = TransactionOrderedDumpFilter(None,q1)
	
	transaction_read_filter = TransactionReadFilter(q1,q2)
	edge_combine_filter = EdgeCombineFilter(q2,q3)
	btc_unit_convert_filter = BtcUnitConvertFilter(q3,q4)
	save_var(q4,'BTC_converted.pk')
	generate_cypher_filter = GenerateCypherFilter(q4,q5)
	file_write_filter = FileWriteFilter(q5,None)
	
	transaction_ordered_dump_filter.start()
	print("Run kiya hai vapas... haath mat laga.....")
	print('Starting Read ')
	transaction_read_filter.start()
	print('Starting Edge Combine ')
	edge_combine_filter.start()
	print('Starting Unit Converter ')
	btc_unit_convert_filter.start()
	print('Generating Cypher command')
	generate_cypher_filter.start()
	print("writing to file")
	file_write_filter.start()
									  
	file_write_filter.join()
	
def raw_ordered_transactions_to_csv_main(start_block,end_block,file_name):
	q1 = Queue()
	q2 = Queue()
	transaction_ordered_dump_filter = TransactionOrderedDumpFilter(None,q1,start_block,end_block)
	transaction_read_to_csv_filter = ReadTransactionToCsvFilter(q1,q2,file_name)
	
	transaction_ordered_dump_filter.start()
	transaction_read_to_csv_filter.start()
	transaction_read_to_csv_filter.join()
	
def raw_ordered_outputs_to_csv_main(start_block,end_block,file_name):
	q1 = Queue(maxsize=100)
	q2 = Queue()
	q3 = Queue()
	q4 = Queue()
	
	transaction_ordered_dump_filter = TransactionOrderedDumpFilter(None,q1,start_block,end_block)
	transaction_read_filter = TransactionReadFilter(q1,q2)
	edge_combine_filter = EdgeCombineFilter(q2,q3)
	btc_unit_convert_filter = BtcUnitConvertFilter(q3,q4)
	read_output_to_csv_filter = ReadOutputToCsvFilter(q4,None,file_name)
	
	transaction_ordered_dump_filter.start()
	transaction_read_filter.start()
	edge_combine_filter.start()
	btc_unit_convert_filter.start()
	read_output_to_csv_filter.start()
	
	read_output_to_csv_filter.join()
	
def raw_ordered_addresses_to_csv_main(start_block,end_block,file_name):
	q1 = Queue()
	q2 = Queue()
	transaction_ordered_dump_filter = TransactionOrderedDumpFilter(None,q1,start_block,end_block)
	address_read_to_csv_filter = ReadAddressToCsvFilter(q1,q2,file_name)
	
	transaction_ordered_dump_filter.start()
	address_read_to_csv_filter.start()
	
	address_read_to_csv_filter.join()

def raw_ordered_inputs_to_csv_main(start_block,end_block,file_name):

	q1 = Queue(maxsize=100)
	q2 = Queue()
	q3 = Queue()
	q4 = Queue()
	
	transaction_ordered_dump_filter = TransactionOrderedDumpFilter(None,q1,start_block,end_block)
	transaction_read_mongo_filter = TransactionReadMongoFilter(q1,q2)
	edge_combine_filter = EdgeCombineFilter(q2,q3)
	btc_unit_convert_filter = BtcUnitConvertFilter(q3,q4)
	read_input_to_csv_filter = ReadInputToCsvFilter(q4,None,file_name)
	
	transaction_ordered_dump_filter.start()
	transaction_read_mongo_filter.start()
	edge_combine_filter.start()
	btc_unit_convert_filter.start()
	read_input_to_csv_filter.start()
	
	read_input_to_csv_filter.join()
	


def populate_utxo_main(start_block,end_block):
	q1 = Queue(maxsize=100)

	transaction_ordered_dump_filter = TransactionOrderedDumpFilter(None,q1,start_block,end_block)
	write_utxo_filter = WriteUtxoFilter(q1,None)
	
	transaction_ordered_dump_filter.start()
	write_utxo_filter.start()
	
	write_utxo_filter.join()
	
def write_to_neo4j():
	directory = "query_files"
	command = "cypher-shell -u neo4j -p bctvjti < "+directory+"/"
	
	i = 0
	while(True):
		file_name = "queries"+str(i)+".cypher"
		print("Running "+file_name)
		try:
			subprocess.call(command+file_name,shell=True)
		except:
			return
		i += 1
		
def api_dump_to_json_main():
	q1 = Queue()
	q2 = Queue()
	q3 = Queue()
	
	transactions = dict()
	api_transaction_dump_filter = ApiTransactionDumpFilter(None,q1)
	edge_combine_filter = EdgeCombineFilter(q1,q2)
	btc_unit_convert_filter = BtcUnitConvertFilter(q2,q3)

	api_transaction_dump_filter.start()
	edge_combine_filter.start()
	btc_unit_convert_filter.start()
	
	
	btc_unit_convert_filter.join()
	
	transactions = dict()
	while(not q3.empty()):
		transaction = q3.get()
		if(transaction is None):
			break
		transactions[transaction["hash"]] = transaction
		
	json_string = json.dumps(transactions)
	
	file = open("transactions.json","w")
	file.write(json_string)
	file.close()
	
	addresses = set()
	for value in transactions.values():
		for inp in value["inputs"]:
			addresses.add(inp[0])
			
		for out in value["outputs"]:
			addresses.add(out[0])

	addresses_info = dict()
	for address in addresses:
		addresses_info[address] = dict()
		addresses_info[address]["in_input"] = []
		addresses_info[address]["in_output"] = []
	
	
	for value in transactions.values():
		for inp in value["inputs"]:
			addresses_info[inp[0]]["in_input"].append((value["hash"],inp[1]))
			
		for out in value["outputs"]:
			addresses_info[out[0]]["in_output"].append((value["hash"],out[1]))
			
	json_string = json.dumps(addresses_info)
	
	file = open("addresses_info.json","w")
	file.write(json_string)
	file.close()


		
if(__name__ == "__main__"):
	start_block = int(sys.argv[1])
	end_block = int(sys.argv[2])
	file_name = sys.argv[3]

	raw_ordered_inputs_to_csv_main(start_block,end_block,file_name)
	
