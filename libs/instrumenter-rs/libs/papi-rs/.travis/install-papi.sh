mkdir -p papi
cd papi
wget http://icl.cs.utk.edu/projects/papi/downloads/papi-5.2.0.tar.gz
tar xzf papi-5.2.0.tar.gz
cd papi-5.2.0/src
./configure --prefix=/usr
make
sudo make install
