# Main script that launches a Succinct cluster:
# - Starts the master on this node.
# - Starts a handler on each node specified in conf/hosts, as well as
# launching logical shards on those nodes.

sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"

. "$sbin/succinct-config.sh"

# Start Servers
echo "Starting servers..."
"$sbin"/start-servers.sh

# Wait for some time
sleep 1

# Start Handlers
echo "Starting handlers..."
"$sbin"/start-handlers.sh

# Wait for some time
sleep 1

# Start Master
echo "Starting master..."
"$sbin"/start-master.sh
