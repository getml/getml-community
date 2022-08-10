# To use docker on macOS, the desktop application has to be started
# be
# Copies all dependencies of the engineforehand. This function checks whether this did already happen and
# do so if not.
function macos_start_docker() {

    if [[ "$OSTYPE" == "darwin"* ]];then
        if [ "$(ps aux | grep com.docker. | wc -l)" -lt "3" ];then
            sudo open --background -a Docker || exit 1

            # Give docker some time to start up.
            sleep 1
        fi
    fi

}
