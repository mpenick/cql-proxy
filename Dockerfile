FROM alpine:3.13 AS builder

RUN apk add --update alpine-sdk cmake autoconf openssl-dev libuv-dev zlib-dev

# Build jemalloc
RUN git clone --branch 5.2.1 https://github.com/jemalloc/jemalloc.git
RUN cd jemalloc; ./autogen.sh && make && make install; cd -

# Build cpp-driver
RUN git clone --depth 1 --branch raw-interface https://github.com/datastax/cpp-driver.git
RUN mkdir cpp-driver/build; cd cpp-driver/build; cmake -DCMAKE_BUILD_TYPE=Release -DCASS_BUILD_STATIC=On .. && make -j8 && make install; cd -

# Build cql-proxy
RUN mkdir -p cql-proxy/src
COPY CMakeLists.txt cql-proxy
COPY src cql-proxy/src
RUN mkdir cql-proxy/build; cd cql-proxy/build; cmake -DCMAKE_BUILD_TYPE=Release .. && make VERBOSE=1; cd -

FROM alpine:3.13

RUN apk add --update openssl libuv zlib libstdc++

COPY --from=builder /cql-proxy/build/cql-proxy cql-proxy
COPY --from=builder /usr/local/lib/libjemalloc.so  /usr/local/lib/libjemalloc.so
RUN ln -s /usr/local/lib/libjemalloc.so /usr/local/lib/libjemalloc.so.2
CMD ["sh", "-c", "./cql-proxy --bundle /secure-connect-bundle.zip --username token --password $TOKEN --bind 0.0.0.0"]
