/**
 *    \file   test.cpp
 *    \brief  a
 *    \date   12/21/2011
 *    \author kanif (xjtukanif@gmail.com)
 */

#include <stdarg.h>
#include <stdio.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdio.h>
#include "SXml.h"

using namespace std;

int handlePaXml(SXmlNode &root)
{
    SXmlNode node = root.GetChildren("url");    
    if(node.Null()){
	    return -1;
    }
    string url = node.GetContent();

    node = root.GetChildren("fetch_requests");
    SXmlNode subitem;
    if(!node.Null()){
	for(SXmlNode item = node.GetChildren(); !item.Null(); item = item.GetNext())
	{
	    string fetch_url;
	    if(item.GetName() != "fetch"){
		fprintf(stderr, "Invalid fetch_requests\n");
		continue;
	    }
	    fetch_url = item.GetChildren("url").GetContent(); 
	    string url_typename;
	    string meta_info;
	    
	    subitem = item.GetChildren("metainfo");
	    if(!subitem.Null())
		meta_info = subitem.GetContent(); 
	    subitem = item.GetChildren("typename");
	    if(!subitem.Null()){
		url_typename = subitem.GetContent();
	    }
	    string attach = "typename=" + url_typename + "; metainfo=" + meta_info + "; " + "sourceurl=" + url;
	    fprintf(stderr, "PA_REQUEST %s\t%s\n", fetch_url.c_str(), attach.c_str());
	}
    }
	
    node = root.GetChildren("categories");
    if(!node.Null()){
	//从商品中得到的面包屑信息
	string goods_types;
	for(SXmlNode item = node.GetChildren(); !item.Null(); item = item.GetNext()){
	    if(item.GetName() != "category")
		continue;
	    string cat_url = item.GetChildren("url").GetContent();
	    string cat_name = item.GetChildren("c").GetContent();
	    utf8_to_gbk(cat_name, cat_name);

	    fprintf(stderr, "CATE_INFO %s\t%s\t%s\n", cat_url.c_str(), cat_name.c_str(), url.c_str());
	    goods_types += cat_name + "\t";

	}
	fprintf(stderr, "ITEM_CAT %s\t%s\n", url.c_str(), goods_types.c_str());
    }

    node = root.GetChildren("shop_info");
    if(!node.Null()){
	//淘宝、拍拍的商家信息
	string shop_url = node.GetChildren("url").GetContent();
	int grade = atoi(node.GetChildren("grade").GetContent().c_str());
	int item_count = atoi(node.GetChildren("item_count").GetContent().c_str());
	int favorite_count = atoi(node.GetChildren("favorite_count").GetContent().c_str());
	fprintf(stderr, "SHOP_INFO %s grade=%d;item_count=%d;favorite_count=%d\n", shop_url.c_str(), grade, item_count, favorite_count);
    }
    return 0;
}

int handleConsumerXml(SXmlNode &root)
{
    //TODO
    //if feed info , just log
    //if new link, pass to next
    return 0;
}

int handleFetchXml(SXmlNode &root)
{
    //TODO
    //pass to next
    //if pass by mapreduce should not filter
    return 0;
}

int handleXml(const string& key, const string& value)
{
    SXmlDocument xml_doc;
    if(xml_doc.Parse(value, "gb18030") != 0){
	return -1;
    }

    SXmlNode xml_root = xml_doc.GetRoot();
    string root_name = xml_root.GetName();
    if(root_name == "pa_feedback"){
	return handlePaXml(xml_root);
    }else if(root_name == "cs_feedback"){
	return handleConsumerXml(xml_root);
    }else if(root_name == "need_fetch"){
	return handleFetchXml(xml_root);
    }
    
    return -1;
}



int main(int argc, char *argv)
{
    string key = "pa_feedback";
    string value = "<?xml version=\"1.0\" encoding=\"gb18030\"?><pa_feedback><url>http://www.360buy.com/product/235950.html</url><fetch_requests><fetch><url>http://img10.360buyimg.com/n5/731/57cdad46-9789-4073-af93-19088c400ccb.jpg</url><typename>thumbnail</typename></fetch></fetch_requests><categories><category><c>首页</c><url>http://www.360buy.com</url></category><category><c>礼品箱包、钟表首饰</c><url>http://www.360buy.com/watch.html</url></category><category><c>钟表</c><url>http://www.360buy.com/products/1672-1673-000.html</url></category><category><c>日本品牌</c><url>http://www.360buy.com/products/1672-1673-1677.html</url></category><category><c>西铁城</c><url>http://www.360buy.com/brands/1677-638.html</url></category><category><c>西铁城光动能男表ｂｒ００２０－５２ｌ</c><url>http://www.360buy.com/product/235950.html</url></category></categories></pa_feedback>";
    
    if(handleXml(key, value) != 0){
	fprintf(stderr, "Invalid_XML %s %s\n", key.c_str(), value.c_str());
    }
    return 0;
}
