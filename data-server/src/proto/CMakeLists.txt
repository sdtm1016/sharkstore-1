set (proto_SOURCES
    gen/ds_admin.pb.cc
    gen/errorpb.pb.cc
    gen/eventpb.pb.cc
    gen/funcpb.pb.cc
    gen/kvrpcpb.pb.cc
    gen/metapb.pb.cc
    gen/mspb.grpc.pb.cc
    gen/mspb.pb.cc
    gen/raft_cmdpb.pb.cc
    gen/schpb.pb.cc
    gen/statspb.pb.cc
    gen/taskpb.pb.cc
    gen/timestamp.pb.cc
    gen/watchpb.pb.cc
)

add_library(sharkstore-proto STATIC ${proto_SOURCES})
