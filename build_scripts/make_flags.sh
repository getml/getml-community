# If no input was provided, display the individual options.
if [ $# -eq 0 ]; then
    echo "usage $0 [cmds list]"
    echo "cmds may be"
    echo "   app           => build the app (the main entry point)"
    echo "   b[undle]      => builds the bundle without compilation"
    echo "   c[ompile]     => compile the source code of the engine"
    echo "   i[nit]        => setup the clean repo for compilation"
    echo "   p[kg]         => build source package"
    echo "   x             => execute getML"
    echo "   z[ip]         => Zip the files for the Docker container"

    exit 1
fi

# --------------------------------------------------------------------

for arg in $@; do

    # Sicne the `branch` command is the only one for which a custom
    # user input is required, it has to be treated differently.
    if [ "$(echo $arg | grep '^branch=' | wc -l)" -gt "0" ];then

        # Trim the prefix to obtain the name of the branch.
        BRANCH_NAME=$(echo $arg | awk 'BEGIN { FS="=" } { print $2 }')

        # Sanity check
        if [ "$(echo $BRANCH_NAME | grep '"' | wc -l)" -gt "0" ];then
            echo -e "\n * ${COLOR_RED}Usage of quotes are forbidden in the branch name!${COLOR_RESET}\n"
            exit 1
        fi

        git_branch_update $BRANCH_NAME

    elif [[ $arg == "doc" ]]; then

        build_documentation

    elif [[ $arg == "init_docker" ]]; then

        init_docker 

    elif [[ $arg == "release" ]]; then

        release

    elif [ $arg != "x" ] && [ $arg != "s" ] && [ $arg != "serve" ] && [ "$USER" != "" ] && [ "$OSTYPE" == "linux-gnu" ]; then
        echo " * Launching docker..."

        docker run -it --rm --ulimit memlock=-1 \
            -v "$PWD/..":"/home/getml/storage" \
            -w "/home/getml/storage/$FOLDER" \
            ${DOCKER_IMAGE_NAME} bash build.sh $arg 

    else

        case $arg in
            app|main_entry_point)
                cmd="build_main_entry_point";;
            b|bundle)
                cmd="build_bundle";;
            c|compile)
                cmd="compile_engine";;
            docker|init_docker)
                cmd="init_docker";;
            i|init)
                cmd="init_repositories";;
            p|pkg)
                cmd="build_package";;
            r|release)
                cmd="release";;
            x)
                cmd="exec_getml";;
            z|zip)
                cmd="zip_docker";;
            *)
                echo -e "${COLOR_RED}unknown command ${arg}${COLOR_RESET}" && exit 1
        esac
        eval $cmd

    fi
done
