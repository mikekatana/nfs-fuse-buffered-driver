# nfs-fuse-buffered-driver

----Tested on Ubuntu Linux 24.04----

The code is actually a prototype and optimized for some synthetic FIO runs


# Install depedencies to compile the FUSE driver

sudo apt update
sudo apt install build-essential cmake libfuse3-dev libnfs-dev pkg-config

# Compile the Code

copy the file nfs_fuse_fio_safe.c in a directory a local drive

Compile the code
gcc -Wall -O3 nfs_fuse_fio_safe.c `pkg-config fuse3 libnfs --cflags --libs` -lpthread -o nfs_fuse_fio_safe

Prepare the mount to your NFS Storage

sudo mount -t nfs -o nconnect=16,vers=3 192.168.60.10:/[EXPORT] /mnt/[MOUNTPOINT]
Create an environment variable which is required for the FUSE drive to listen on

export NFS_URL="nfs://192.168.60.10/[EXPORT]

# RUN the FUSE DRIVER

./nfs_fuse_fio_safe /mnt/[MOUNTPOINT] -f -d -o max_threads=32

