#! /bin/bash

# By: pengtao@baidu.com
# Create date:    21 Oct 2014
# Last modified:  

###################################################
# purpose:
#      ��ftp����ͼ���أ�����bannerƴ�ӣ������ϴ���ȥ
#
###################################################


DATE=PO7013  # ��ƷĿ¼������)������ָ��

if [ $# -ge 1 ] ; then
    DATE=$1
fi


# ftp·��
WORK_BASE='/home/work/mua/uploads'
# ���ͼ��ͷͼ��·��
AD_PATH='/home/work/mua/tools/ad_banner.JPG'

BIN='/home/service/imageMagick/bin/convert'

# debug ����
# DATE=tmp  
# WORK_BASE='/home/work/taopeng/projects/20141021-mua-image-concat'
# AD_PATH='/home/work/taopeng/projects/20141021-mua-image-concat/ad_banner.JPG'

# ͼƬ���
WIDTH=640
# ����ͼ�߶�
HIGHT_AD=360
# ��Ʒͼ�߶�
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
