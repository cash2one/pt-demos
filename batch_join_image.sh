#! /bin/bash

# By: pengtao@baidu.com
# Create date:    21 Oct 2014
# Last modified:  

###################################################
# purpose:
#      从ftp将首图下载，与广告banner拼接，重新上传回去
#
###################################################


DATE=PO7013  # 货品目录（期数)用日期指代

if [ $# -ge 1 ] ; then
    DATE=$1
fi


# ftp路径
WORK_BASE='/home/work/mua/uploads'
# 广告图（头图）路径
AD_PATH='/home/work/mua/tools/ad_banner.JPG'

BIN='/home/service/imageMagick/bin/convert'

# debug 配置
# DATE=tmp  
# WORK_BASE='/home/work/taopeng/projects/20141021-mua-image-concat'
# AD_PATH='/home/work/taopeng/projects/20141021-mua-image-concat/ad_banner.JPG'

# 图片宽度
WIDTH=640
# 海报图高度
HIGHT_AD=360
# 商品图高度
HIGHT_GOODS=900


WPATH=$WORK_BASE/$DATE
HIGHT=`expr $HIGHT_GOODS + $HIGHT_AD`

for gid in `ls $WPATH`; do
    gid_path=$WPATH/${gid}
    echo $gid_path
    if [ ! -f $gid_path/2.JPG ] ; then
        echo "generating ${gid_path}/2.JPG ..."
        set -x
        $BIN -size ${WIDTH}x${HIGHT} xc:#ffffff  -draw "image Over 0,0 ${WIDTH},${HIGHT_AD} \"${AD_PATH}\"" -draw "image Over 0,${HIGHT_AD} ${WIDTH},${HIGHT_GOODS} \"${gid_path}/1.jpg\""   $gid_path/2.jpg
        set +x
    fi
done
