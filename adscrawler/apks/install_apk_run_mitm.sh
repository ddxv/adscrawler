#!/bin/bash

set -e

Help() {
	# Display Help
	echo "Description of the script functions."
	echo
	echo "Syntax: scriptTemplate [-h|s <store_id>]"
	echo "options:"
	echo "h     Print this Help."
	echo "s     Enter a store_id."
	echo
}

while getopts hs: option; do
	case $option in
	h) # display Help
		Help
		exit
		;;
	s) # Enter a store_id
		app=$OPTARG ;;
	\?) # Invalid option
		echo "Error: Invalid option"
		exit
		;;
	esac
done

if [ -z "$app" ]; then
	echo "${app:?Missing -s}"
else
	echo "App is set to: $app"
fi

waydroidinstalledapps=$(waydroid app list)

linecount=$(echo "$waydroidinstalledapps" | grep -c "packageName: $app" || true)
if [ "$linecount" = 0 ]; then
	echo "store_id: $app not yet installed"
    echo "store_id: $app installing"
    waydroid app install "/home/james/apk-files/apks/$app.apk"
	sleep 5
    waydroidinstalledapps=$(waydroid app list)
    linecount=$(echo "$waydroidinstalledapps" | grep -c "packageName: $app" || true)
    if [ "$linecount" = 0 ]; then
	    echo "Matches $linecount already installed apps: $lines"
	else
	    echo "failed to install"
		exit
    fi
else
	lines=$(echo "$waydroidinstalledapps" | grep "packageName: $app")
	echo "Matches $linecount already installed apps: $lines"
fi

adscrawler/apks/mitm_start.sh -d
sleep 2


echo "Setting up MITM proxy for $app..."
adscrawler/apks/mitm_start.sh -w -s "$app" & proxy_pid=$!

# Give the proxy a moment to start up, will ask for sudo password
sleep 20
waydroid app launch "$app"

sleep 2

if ! ps -p $proxy_pid > /dev/null; then
    echo "Error: MITM proxy failed to start"
    exit 1
fi

echo "MITM proxy started with PID $proxy_pid"

echo "Launching $app..."
waydroid app launch "$apk_path"

echo ""
echo "The MITM proxy is running in the background with PID $proxy_pid"
echo "When you're done, you can stop it with: kill $proxy_pid"
echo "And clean up the iptables rules with: ./mitm_start.sh -d"


# This is our duration of capture
sleep 60


# Kill the proxy process
echo "Stopping mitmproxy..."
kill $proxy_pid

# Clean up iptables rules
echo "Cleaning up iptables rules..."
./adscrawler/apks/mitm_start.sh -d

# Uninstall the app
echo "Uninstalling app $app..."
waydroid app remove "$app"

echo "Process complete. Traffic log saved."
