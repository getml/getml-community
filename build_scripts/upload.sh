# Upload a file to the S3 bucket defined in the beginning of this file.
function upload() {
    echo -e "\n * ${COLOR_GREEN}Uploading file to ${S3_BUCKET}...${COLOR_RESET}\n"

    if [ "$(which aws | wc -l)" -lt "1" ];then
        echo -e "\n * ${COLOR_RED}No AWS CLI found!${COLOR_RESET}\n"
        exit 1
    fi

    # ----------------------------------------------------------------

    if [ ! -f $1 ];then
        echo -e "\n * ${COLOR_RED}File $1 does not exist!${COLOR_RESET}\n"
        exit 1
    fi

    # ----------------------------------------------------------------

    aws s3 cp $1 s3://${S3_BUCKET} || exit 1

    # ----------------------------------------------------------------

}
