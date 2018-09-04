# 依赖
## grpc 
v1.6.4
```sh
git clone --depth 1 --branch v1.6.4 https://github.com/grpc/grpc.git
cd grpc
git submodule update --init
make -j `nproc`
sudo make install
```

## protobuf 
v3.4.1
```sh
git clone --depth 1 --branch v3.4.1 https://github.com/google/protobuf.git
cd protobuf
./autogen.sh
./configure
make -j `nproc`
sudo make install
```

## libfastcommon
V1.0.36
```sh
git clone --depth 1 https://github.com/ChenVsGuo/libfastcommon.git
cd libfastcommon
./make.sh
sudo ./make.sh install
```

## snappy
```sh
wget https://github.com/google/snappy/archive/1.1.7.tar.gz
tar xvf 1.1.7.tar.gz
cd snappy-1.1.7/
mkdir build && cd build
cmake .. -DSNAPPY_BUILD_TESTS=OFF -DCMAKE_CXX_FLAGS="-O2 -DNDEBUG -g -fPIC"
sudo make install -j 4
```

## rocksdb
v5.11.3
```sh
wget https://github.com/facebook/rocksdb/archive/v5.11.3.tar.gz
tar xvf v5.11.3.tar.gz
cd rocksdb-5.11.3
mkdir build
cd build
cmake .. -DWITH_SNAPPY=ON -DWITH_TESTS=OFF
make -j `nproc`
sudo make install
```

## rapidjson安装
```sh
git clone --depth 1 --branch v1.1.0 https://github.com/Tencent/rapidjson.git
sudo cp -r rapidjson/include/rapidjson /usr/local/include/
```

## asio安装   
```sh
wget https://github.com/chriskohlhoff/asio/archive/asio-1-12-0.tar.gz
tar xvf asio-1-12-0.tar.gz
cd asio-asio-1-12-0/asio
./autogen.sh
env CXXFLAGS="-DASIO_STANDALONE -std=c++11" ./configure --without-boost
sudo make install -j `nproc`
```

## gtest and gmock
v1.8.0
```sh
wget https://github.com/google/googletest/archive/release-1.8.0.tar.gz -O gtest18.tar.gz
tar xvf gtest18.tar.gz
cd googletest-release-1.8.0
cmake .
make -j `nproc`
sudo make install
```

## gperf tools
v2.6.3
```sh
wget https://github.com/gperftools/gperftools/archive/gperftools-2.6.3.tar.gz
tar xvf gperftools-2.6.3.tar.gz
cd gperftools-gperftools-2.6.3
./autogen.sh
./configure
make -j `nproc`
sudo make install
```

# 编译data-server
```sh
cd data-server
mkdir build
cd build
cmake ..
make -j `nproc`
```

## 编译测试

### 安装asio网络库 (raft测试程序需要）
```sh
wget https://github.com/chriskohlhoff/asio/archive/asio-1-10-8.tar.gz
cd asio-asio-1-10-8/asio
./autogen.sh
./configure --without-boost
make -j `nproc`
sudo make install
```

### 编译dataserver带tests

```sh
cd data-server
mkdir build
cd build
cmake .. -DFBASE_BUILD_TEST=1
make -j `nproc`
```

## 单测覆盖率
install gcov lcov    
带选项编译 `cmake -DENABLE_COVERAGE=ON ..`  会生成.gcno文件      
正常运行`./data-server ../conf/ds.conf start` 会生成.gcda文件      
`find ./ -name "*.gcno" | xargs gcov`  会生成.gcov文件      
将测试结果合并到一个文件 `lcov -d . -c -o ds.gcov.info   `   
将结果文件转换成html格式，输出到ds_report目录  `genhtml ds.gcov.info -o ds_report`  
浏览器运行ds_report的index.html文件查看结果       
