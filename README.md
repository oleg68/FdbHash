# FdbHash
A tool for calculating checksums and comparing two foundationdb databases

# Building
1. Clone the source repository
    git clone https://github.com/oleg68/FdbHash.git
2. Go to the local repository directory
    cd FdbHash
3. Execute
    mvn install
  The resulting jar will be installed to the .m2/repository/com/openwaygroup/dbkernel/fdb/fdb-hash subdirectory of your home directory.
  The required fdb client library will be copied to the target/lib subdirectory

# Deploying
1. Make a directory for deploying. It is named as ``Delploy Directory``.
2. Make a subdirectory ``lib`` of ``Delploy Directory``.
3. Copy fdb-hash-0.2.jar from .m2/repository/com/openwaygroup/dbkernel/fdb/fdb-hash/0.2/ subdirectory of your home directory to ``Delploy Directory``.
4. Copy target/lib/fdb-java-6.2.10.jar to ``Delploy Directory``/lib

# Usage
   See [usage.txt](src/main/resources/usage.txt)
