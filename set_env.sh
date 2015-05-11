# $1 - Last digits of first machine IP
# $2 - galera IP_end

IP_end=$1
galeraIP=$2

if [ -z $galeraIP ] ; then
	galeraIP=`expr $IP_end + 5`
fi

r1=`expr $IP_end + 1`
r2=`expr $IP_end + 2`
r3=`expr $IP_end + 3`
r4=`expr $IP_end + 4`

g1=`expr 1 + $galeraIP`
g2=`expr 2 + $galeraIP`
g3=`expr 3 + $galeraIP`
g4=`expr 4 + $galeraIP`

# Number of nodes
export galera_N=4
export repl_N=4

# IP of Master/Slave replication setup nodes
export repl_000="192.168.122.$r1"
export repl_001="192.168.122.$r2"
export repl_002="192.168.122.$r3"
export repl_003="192.168.122.$r4"

# IP of Galera cluster nodes
export galera_000="192.168.122.$g1"
export galera_001="192.168.122.$g2"
export galera_002="192.168.122.$g3"
export galera_003="192.168.122.$g4"

# MariaDB/Mysql port of of Master/Slave replication setup nodes
export repl_port_000=3306
export repl_port_001=3306
export repl_port_002=3306
export repl_port_003=3306

# MariaDB/Mysql Galera cluster nodes
export galera_port_000=3306
export galera_port_001=3306
export galera_port_002=3306
export galera_port_003=3306



export maxdir="/usr/local/mariadb-maxscale"
export maxscale_cnf="$maxdir/etc/MaxScale.cnf"
export log_dir="$maxdir/logs/"
export sysbench_dir="/home/ec2-user/sysbench_deb7/sysbench/"
if [ -f copy_logs.sh ] ; then
	export test_dir=`pwd`
else
	export test_dir=$maxdir/system-test/
fi

if [ "$new_dirs" == "yes" ] ; then
	export maxdir="/usr/local/mariadb-maxscale"
	export maxscale_cnf="/etc/MaxScale.cnf"
	export log_dir="/var/log/maxscale/"
fi

# IP Of MaxScale machine
export maxscale_IP="192.168.122.$IP_end"

# User name and Password for Master/Slave replication setup (should have all PRIVILEGES)
export repl_user="skysql"
export repl_password="skysql"

# User name and Password for Galera setup (should have all PRIVILEGES)
export galera_user="skysql"
export galera_password="skysql"

export maxscale_user="skysql"
export maxscale_password="skysql"

export maxadmin_password="mariadb"

# command to kill VM (obsolete)
export kill_vm_command="/home/ec2-user/test-scripts/kill_vm.sh"
# command to restore VM (obsolete)
export start_vm_command="/home/ec2-user/test-scripts/start_vm.sh"

export repl_kill_vm_command="/home/ec2-user/test-scripts/kill_vm.sh"
export repl_start_vm_command="/home/ec2-user/test-scripts/start_vm.sh"
export galera_kill_vm_command="/home/ec2-user/test-scripts/kill_vm.sh"
export galera_start_vm_command="/home/ec2-user/test-scripts/start_vm.sh"

# command to download logs from MaxScale machine
export get_logs_command="$test_dir/get_logs.sh"

# used to generate links to ssh keys on Jenkins machine
export ImagesDir="/home/ec2-user/kvm/images"
export SSHKeysDir="/home/ec2-user/KEYS"
export TestVMsDir="/home/ec2-user/test-machines"

# links to ssh keys files for all machines
export repl_sshkey_000=$SSHKeysDir/`cat $TestVMsDir/image_name_$repl_000`
export repl_sshkey_001=$SSHKeysDir/`cat $TestVMsDir/image_name_$repl_001`
export repl_sshkey_002=$SSHKeysDir/`cat $TestVMsDir/image_name_$repl_002`
export repl_sshkey_003=$SSHKeysDir/`cat $TestVMsDir/image_name_$repl_003`

export galera_sshkey_000=$SSHKeysDir/`cat $TestVMsDir/image_name_$galera_000`
export galera_sshkey_001=$SSHKeysDir/`cat $TestVMsDir/image_name_$galera_001`
export galera_sshkey_002=$SSHKeysDir/`cat $TestVMsDir/image_name_$galera_002`
export galera_sshkey_003=$SSHKeysDir/`cat $TestVMsDir/image_name_$galera_003`

export maxscale_sshkey=$SSHKeysDir/`cat $TestVMsDir/image_name_$maxscale_IP`

# Sysbench directory (should be sysbench >= 0.5)
export sysbench_dir="/home/ec2-user/sysbench_deb7/sysbench/"
