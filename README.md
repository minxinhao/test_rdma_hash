# TEST_RDMA_HASH

Test the performance of RACE, Pilaf , DrTM Cluster Hashing and Farm Hotspotch Hashing implemented in One-Side RDMA and One-Side RDMA based RPC.

Implementing eRPC, RFP , ScaleRPC to test performance in different situations.


## Bug

### RACE

#### Get Miss
Problem：
1. Get读取了整个Bucket到本地，
2. 遍历bucket的过程中，远端有并发的insert写入，
3. 但是并没有触发split
4. 本地的Get无法通过Re-read来感知，发生get-miss

Solution：
1.Get逐个读取远端的slot？（Insert/Update/Split对Slot的cas操作能保证对RDMA的原子性）


#### RDMA Read不一致性
RDMA Read读取到被insert/delete/update/split修改的中间状态，会有这种情况吗？就是上面说的Insert/Update/Split对Slot的cas操作能保证对RDMA的原子性吗？

如果不能，感觉存在大量的一致性开销。