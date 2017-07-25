# intermix puts, gets and rebalances

readallfields=true

readproportion=0.5
updateproportion=0.5
scanproportion=0
insertproportion=0


# the next setting wierdly control whether the input key will be hashed or not.
# if it is (the default!), the input will be spread over a 64-bit range of numbers.
# if it is set to something other than "hashed" (the default), the input's range
# will be max(record_count_, op_count * insert_proportion) * 2
insertorder=dont_hash

#piwi specific
initChunks=5000
workerThreadsNum=4
munkKeyBytes=14
exact_key_size=14
fieldlength=800
funkBytesCapacity=1000000000

#rocks specific
rocksdb_syncwrites=true
compression=no
write_buffer_size=536870912
max_write_buffer_number=5
min_write_buffer_number_to_merge=2

#also available
#dbdir=/homes/erangi/ycsbc
keyrange=15000000

dataDir=baseline_data_piwi
