#!/bin/bash
set -e

# check protoc exist
command -v protoc >/dev/null 2>&1 || { echo >&2 "ERR: protoc is required but it's not installed.  Aborting."; exit 1; }

# find protoc-gen-gofast
GOGO_GOPATH=""
for path in $(echo "${GOPATH}" | sed -e 's/:/ /g'); do
    gogo_proto_bin="${path}/bin/protoc-gen-gofast"
    if [ -e "${gogo_proto_bin}" ]; then
        export PATH=$(dirname "${gogo_proto_bin}"):$PATH
        GOGO_GOPATH=${path}
        break
    fi
done
# protoc-gen-gofast not found
if [[ -z ${GOGO_GOPATH} ]]; then
    echo >&2 "ERR: Could not find protoc-gen-gofast"
    echo >&2 "Please run \`go get github.com/gogo/protobuf/protoc-gen-gofast\` first"
    exit 1;
fi
echo "gogo found in gopath: "${GOGO_GOPATH}


cd `dirname $0`"/proto"
echo "generate go code..."
for file in `ls *.proto`; do
    base_name=$(basename $file ".proto")
    out_dir="../pkg/"${base_name}
    mkdir -p ${out_dir}
    out_file=${out_dir}"/"${base_name}".pb.go"
    protoc -I.:${GOGO_GOPATH}/src/github.com/gogo/protobuf --gofast_out=plugins=grpc:${out_dir} $file
    # fix import path
    # for compatible with sed's different versions, we use a tmpfile
    sed 's/import \([^ ]*\) "\."/import \1 "model\/pkg\/\1"/' ${out_file} > tmpfile
    mv tmpfile ${out_file}
done
