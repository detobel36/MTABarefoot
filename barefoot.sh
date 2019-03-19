#!/bin/bash

################ Constant ################

GIT_REPOS='https://github.com/bmwcarit/barefoot.git'
BAREFOOT_NAME='barefoot-instance'

DEFAULT_USER='rdetobel'

DATABASE_NAME='barefootdb'
DATABASE_USER='osmuser'
DATABASE_PASS='pass'


################ Functions ################

# Function getAttribute
# Permet de demander à l'utilisateur de rentrer un argument
# qui est valide
#
# @param $1 liste des choix valides
# @param $2 phrase a afficher lors de l'entrée
getAttribute() {
    valide=0
    while [ $valide != 1 ]
    do
        read -p "$2" input

        for element in $1
        do
            if [ "$input" == "$element" ]
            then
                valide=1
                break
            fi
        done

        if [ $valide != 1 ]
        then
            echo "Input not valid"
        fi
    done

    retval=$input
}

# Function to create a configuration file
# 
# @param $1 database host
# @param $2 database name
# @param $3 database user
# @param $4 database pass
# @param $5 config name
createConfiguration() {
    echo "database.host=$1
database.port=5432
database.name=$2
database.table=bfmap_ways
database.user=$3
database.password=$4
database.road-types=./map/tools/road-types.json" > "config/$5"
}


manageAction() {
    clear
    echo "---- Barefoot Script Menu ----"
    echo ""
    echo " - pre-install"
    echo "     Clone GitHub repository "
    echo "      and install some software"
    echo ""
    echo " - install"
    echo "     Create and start server"
    echo ""
    echo " - uninstall"
    echo "     Remove all installed files"
    echo ""
    echo " - quit"
    echo "     Quit this script"
    echo ""
    echo "------------------------------"
    echo ""
    getAttribute "pre-install install uninstall quit" "Select action [pre-install|install|uninstall|quit]: "
    action=$retval

    if [ "$action" == "pre-install" ]
    then
        clear
        managePreInstall

    elif [ "$action" == "install" ]
    then
        clear
        manageInstall

    elif [ "$action" == "uninstall" ]
    then
        echo "TODO: not yet available"

    elif [ "$action" == "quit" ]
    then
        # DO nothing
        clear

    else
        echo "Unknow action"
    fi
}


managePreInstall() {
    echo "---- PRE-INSTALL Barefoot ----"
    echo ""
    echo " Some programs needs to be "
    echo "  download and install"
    echo ""
    echo "------------------------------"
    echo ""

    curl -sL https://deb.nodesource.com/setup_9.x | bash -
    apt-get update
    apt-get install git wget docker curl maven openjdk-8-jdk nodejs libzmq3-dev
    apt-get upgrade
    git clone $GIT_REPOS
    chown -R $DEFAULT_USER:$DEFAULT_USER ./
    echo ""
    read -p "Enter to continue" next
    manageAction
}


manageInstall() {
    echo "------ INSTALL Barefoot ------"
    echo ""
    echo " 3 types of servers:"
    echo "  - Map server"
    echo "      Contains maps used to "
    echo "         make map matching"
    echo ""
    echo "  - Match server"
    echo "      Map matching with all"
    echo "         the track"
    echo ""
    echo "  - Track server"
    echo "      Map matching in real-time"
    echo ""
    echo "    Track monitor server (web)"
    echo "      Web server to see in"
    echo "          real-time the evolution"
    echo ""
    echo "------------------------------"
    echo ""

    getAttribute "map match track web back" "Type of server to install [map|match|track|web|back]: "
    choice=$retval


    if [ "$choice" == "map" ] 
    then
        clear
        manageMapServer

    elif [ "$choice" == "match" ]
    then
        manageMatchServer

    elif [ "$choice" == "track" ]
    then
        manageTrackServer

    elif [ "$choice" == "web" ]
    then
        manageTrackWebServer

    elif [ "$choice" == "back" ]
    then
        manageAction

    else
        echo "Unknow type of server"
    fi

}

manageMapServer() {
    echo "----- Map Server Barefoot ----"
    echo ""
    echo " Different map could be install"
    echo " you must therefore provide a"
    echo " link to download the maps of"
    echo " the desired area"
    echo ""
    echo "------------------------------"
    echo ""
    echo "Example for New York:"
    echo "http://download.geofabrik.de/north-america/us-northeast-latest.osm.pbf"
    echo ""

    read -p "URL of the map: " mapUrl

    mapName=`basename -s .pbf "$mapUrl"`
    mapName="$mapName.pbf"
    noDownload=0

    if [ -f "barefoot/map/osm/$mapName" ]
    then
        read -p "File already exist. Delete existing file ? [Yes/no] " delete
        if [ "$delete" == "yes" ] || [ "$delete" == "y" ] || [ "$delete" == "Y" ]
        then
            rm -rf "barefoot/map/osm/$mapName"
        else
            noDownload=1
        fi
    fi

    if [ $noDownload == "0" ]
    then
        curl "$mapUrl" -o "barefoot/map/osm/$mapName"
        chown -R $DEFAULT_USER:$DEFAULT_USER barefoot/map/osm/
    fi


    cd barefoot
    docker build -t barefoot-map ./map
    alreadyExist=`docker ps -a | grep "$BAREFOOT_NAME"`
    if [ "$alreadyExist" != "" ]
    then
        docker stop "$BAREFOOT_NAME"
        docker rm "$BAREFOOT_NAME"
    fi
    docker run -d -p 5432:5432 --name="$BAREFOOT_NAME" -v ${PWD}/map/:/mnt/map barefoot-map
    docker exec -t "$BAREFOOT_NAME" bash -c "service postgresql start"

    read -p "Database name [$DATABASE_NAME]: " dbName
    DATABASE_NAME=${dbName:-"$DATABASE_NAME"}

    read -p "Database user [$DATABASE_USER]: " dbUser
    DATABASE_USER=${dbUser:-"$DATABASE_USER"}

    read -p "Database user [$DATABASE_PASS]: " dbPass
    DATABASE_PASS=${dbPass:-"$DATABASE_PASS"}

    createConfiguration localhost $DATABASE_NAME $DATABASE_USER $DATABASE_PASS "customConfig.properties"
    echo "Auto create new configuration: config/customConfig.properties"
    read -p "Enter to continue" next

    #                                                                input                 database       database user  database pass  config                         mode    
    docker exec -t -d "$BAREFOOT_NAME" bash -c "/mnt/map/osm/import.sh /mnt/map/osm/$mapName $DATABASE_NAME $DATABASE_USER $DATABASE_PASS /mnt/map/tools/road-types.json slim"
    cd ../

    running=`docker inspect -f {{.State.Running}} "$BAREFOOT_NAME"`
    if [ "$running" == "true" ]
    then
        echo "Docker correctly launch !"
    else
        echo "Problem during the installation of the Map Server"
    fi
    echo ""

    read -p "Enter to continue" next
    manageAction
}


packageMaven() {
    cd barefoot/

    if [ -d "./target" ]
    then
        read -p "Force build [Y/n]" forcebuild
        forcebuild=${forcebuild:-"yes"}
    else
        forcebuild="y"
    fi

    if [ "$forcebuild" == "y" ]
    then
        mvn clean package -DskipTests
    fi
}


manageMatchServer() {
    echo "---- Match Server Barefoot ---"
    echo ""
    echo " Server which calculate the"
    echo " map matching"
    echo ""
    echo "------------------------------"

    packageMaven
    jarFile=`ls target/*-matcher-jar-with-dependencies.jar`
    jarFile="${PWD}/$jarFile"

    read -p "Server configuration [server.properties]: " configServer
    configServer=${configServer:-"server.properties"}
    configServer="config/$configServer"

    read -p "Database configuration [customConfig.properties]: " configDb
    configDb=${configDb:-"customConfig.properties"}
    configDb="config/$configDb"

    java -jar "$jarFile" --geojson "$configServer" "$configDb"
}

manageTrackServer() {
    echo "---- Track Server Barefoot ---"
    echo ""
    echo " Server which calculate the"
    echo " new position of given "
    echo " coordinate (in real time)"
    echo ""
    echo "------------------------------"

    packageMaven
    jarFile=`ls target/*-tracker-jar-with-dependencies.jar`
    jarFile="${PWD}/$jarFile"

    read -p "Server configuration [tracker.properties]: " configServer
    configServer=${configServer:-"tracker.properties"}
    configServer="config/$configServer"

    read -p "Database configuration [customConfig.properties]: " configDb
    configDb=${configDb:-"customConfig.properties"}
    configDb="config/$configDb"

    echo "Start track server"
    java -jar "$jarFile" "$configServer" "$configDb" > trackLog.log
}


manageTrackWebServer() {
    cd barefoot/util/monitor
    npm install --unsafe-perm --verbose
    cd ../../
    node util/monitor/monitor.js 3000 127.0.0.1 1235
}



################ MAIN ################

clear

if [[ "$TERM" != screen* ]]
then
    echo "--- Barefoot Script Protype ---"
    echo ""
    if [ "$(id -u)" != "0" ]
    then
        echo " /!\ It is recommended to launch"
        echo "     this script with root"
        echo "     permissions"
        echo ""
    fi
    echo " Start this script in a screen"
    echo "  or with tmux because some"
    echo "  processes will be directly"
    echo "  launched by this script."
    echo ""
    echo "-------------------------------"

    read -p "Enter to continue" next
fi

manageAction
