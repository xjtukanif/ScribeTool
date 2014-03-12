#include "SXml.h"
#include <stdio.h>
#include <errno.h>
#include <cctype>
#include <string.h>

static void toUpper(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =toupper(*p);
	}
}
static void toLower(std::basic_string<char>& s)
{
	for (std::basic_string<char>::iterator p = s.begin();p!=s.end();++p){
		*p =tolower(*p);
	}
}

static int s_iconv(const std::string &src, std::string &result,std::string in,std::string out)
{
	int max_len = src.length()*2+1;
	iconv_t cd = iconv_open(out.c_str(),in.c_str());
	if (cd<0){
		return -1;	
	}
	
	char *buf = new char[max_len];
	if (buf == NULL)
	    return -1;
	
	char *inbuff = const_cast<char *>(src.c_str());

	size_t inbytesleft = src.length();
	char *outbuff = buf;
	size_t outbytesleft = max_len;
	size_t ret = iconv(cd, &inbuff, &inbytesleft, &outbuff, &outbytesleft);

	if(ret == size_t(-1))
	{
	    printf("iconv failed: %s\n", strerror(errno));
	}else {
	    size_t content_len = max_len - outbytesleft;
	    //printf("content_len: %d\n", content_len);
	    //printf("out len : %s\n",strlen(buf));
	    result.assign(buf,content_len);
	}
	iconv_close(cd);
	delete []buf;
	return ret;
}


int utf16_to_utf8(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF16LE","UTF8");
}
int utf8_to_utf16(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF8","UTF16LE");
}
int utf16_to_gbk(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF16LE","GB18030");
}
int gbk_to_utf16(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"GB18030","UTF16LE");
}
int utf8_to_gbk(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"UTF8","GB18030");
}
int gbk_to_utf8(const std::string &src, std::string &result)
{
	return s_iconv(src,result,"GB18030","UTF8");
}

std::string getXmlNodeContent(SXmlNode &node, const std::string &key, const std::string &default_value)
{
    SXmlNode subnode = node.GetChildren(key);
    return subnode.Null()? default_value:subnode.GetContent();
}

std::string getXmlNodeContent(SXmlNode &node, const std::string &key, const char *default_value)
{
    SXmlNode subnode = node.GetChildren(key);
    return subnode.Null()? default_value:subnode.GetContent();
}

bool getXmlNodeContent(SXmlNode &node, const std::string &key, bool default_value)
{
    SXmlNode subnode = node.GetChildren(key);
    if(subnode.Null())
	return default_value;
    std::string tmp_value = subnode.GetContent();
    toLower(tmp_value);
    return (tmp_value == "yes" || tmp_value == "true")? true: false;
}

int getXmlNodeContent(SXmlNode &node, const std::string &key, int default_value)
{
    SXmlNode subnode = node.GetChildren(key);
    return subnode.Null()? default_value:atoi(subnode.GetContent().c_str());
}

long getXmlNodeContent(SXmlNode &node, const std::string &key, long default_value)
{
    SXmlNode subnode = node.GetChildren(key);
    return subnode.Null()? default_value:atol(subnode.GetContent().c_str());
}

SXmlDocument::~SXmlDocument()
{
#ifdef USE_LIBXML
	if (_doc){
	xmlFreeDoc(_doc);
	}
#endif 
}
int SXmlDocument::Parse(const std::string& str,std::string encoding)
{
	int ret=0;
	std::string str_in =str;
	toLower(encoding);
	if (encoding == "utf-16"){	
		_encoding = encoding;
		ret=utf16_to_utf8(str_in,str_in);
		if (ret<0)
			return -1;
		ChangeEncodeDeclaration(str_in,str_in,"utf-16","utf-8");		
	}else if (encoding == "gbk"){
		_encoding = encoding;
		ChangeEncodeDeclaration(str_in,str_in,"gbk","utf-8");		
		ret=gbk_to_utf8(str_in,str_in);
		if (ret<0)
			return -1;
	}else if(encoding == "gb18030"){
		_encoding = encoding;
		ChangeEncodeDeclaration(str_in,str_in,"gb18030","utf-8");		
		ret=gbk_to_utf8(str_in,str_in);
		if (ret<0)
			return -1;
	}else if (encoding == "utf-8"){
		_encoding = encoding;
		ChangeEncodeDeclaration(str_in,str_in,"gbk","utf-8");		
	}else{
		//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::Parse: can not support this encoding %s\n",encoding.c_str()));
		return -1;
	}
	
	_doc = xmlReadMemory(str_in.c_str(), str_in.length(), NULL, "UTF-8", 0);
	if (_doc==NULL)
		return -1;
	
	return 0;
}
int SXmlDocument::ChangeEncodeDeclaration(std::string in,std::string &out,std::string src,std::string des)
{
	//xml version="1.0" encoding="gbk"?>
	int pos = in.find("<?xml");
	int pos2= in.find("?>");
	if ((pos<0 ) ||(pos2<0))
		return -1;
	int pos_encoding = in.find(src,pos);
	if (pos_encoding > pos2)
		return -1;
	//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::ChangeEncodeDeclaration:str_in  %s\n",in.c_str()));
	if (pos_encoding < 0) {
		toUpper(src);
		pos_encoding = in.find(src,pos);
		if ((pos_encoding > pos2)||(pos_encoding <0))
			return -1;
	}
	in.replace(pos_encoding,src.size(),des);
	out = in;
	//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::ChangeEncodeDeclaration:out  %s\n",out.c_str()));
	return 1;
}

SXmlNode SXmlDocument::GetRoot()
{
	if (_doc){		
		SXmlNode node;
		node._node = xmlDocGetRootElement(_doc);
		return node;
	}else {
		SXmlNode node;
		node._node = NULL;
		return node;
	}
}

int SXmlDocument::SaveToString(std::string &str)
{	
	xmlChar *xmlbuff=NULL;
	int buffersize;
	std::string output="";

	if (!_doc)
		return -1;
	int ret=0;
	xmlDocDumpFormatMemory(_doc, &xmlbuff, &buffersize, 1);
	if(!xmlbuff){
		return -1;
	}
	char * buf = (char *)xmlbuff;
	for (int i=0;i<buffersize;i++)
		output += buf[i];
	
	
	//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::SaveToString : %s\n",output.c_str()));
	if (_encoding == "utf-16"){	

		ret = ChangeEncodeDeclaration(output,output,"utf-8","utf-16");		
		ret = utf8_to_utf16(output,output);
		if (ret<0){
			xmlFree(xmlbuff);
			return -1;
		}
			
	}else if (_encoding == "gbk"){
		
		ret = utf8_to_gbk(output,output);
		
		if (ret < 0){
			xmlFree(xmlbuff);
			return -1;
		}
		ret = ChangeEncodeDeclaration(output,output,"utf-8","gbk");		
		//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::SaveToString : %s\n",_encoding.c_str()));
	}else if (_encoding == "utf-8"){

	}else{
	}
	
	str = output;
	xmlFree(xmlbuff);
	
	return 0;
}
int SXmlDocument::SaveToStringWithEncode(std::string &str,std::string out_encode)
{	
	xmlChar *xmlbuff;
	int buffersize;
	std::string output="";
	toLower(out_encode);

	if (!_doc)
		return -1;
	xmlDocDumpFormatMemory(_doc, &xmlbuff, &buffersize, 1);
	char * buf = (char *)xmlbuff;
	for (int i=0;i<buffersize;i++)
		output += buf[i];	
	
	int ret = 0;

	//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::SaveToString : %s\n",output.c_str()));
	if (out_encode == "utf-16"){	

		ret = ChangeEncodeDeclaration(output,output,"utf-8","utf-16");		
		utf8_to_utf16(output,output);
	}else if (out_encode == "gbk" || out_encode == "gb18030"){
		
		utf8_to_gbk(output,output);
		ret = ChangeEncodeDeclaration(output,output,"utf-8",out_encode);		
		//WEBSEARCH_DEBUG((LM_ERROR, "SXmlDocument::SaveToString : %s\n",_encoding.c_str()));
	}else if (_encoding == "utf-8"){

	}else{
	}
	
	str = output;
	xmlFree(xmlbuff);
	
	return ret;
}

/*void SXmlDocument::Free()
{
	if (_doc)
		xmlFreeDoc(_doc);
}*/
bool SXmlNode::Null()
{
	if (_node!=NULL)
		return 0;
	else 
		return 1;
}

void SXmlNode::Remove()
{
	if (_node){
		xmlUnlinkNode(_node);
		xmlFreeNode(_node);
	}
}

std::string SXmlNode::GetName()
{
	if (_node){
		if (_node->name)
			return (char *)_node->name;
		else 
			return "";			
	}else 
		return "";
}
SXmlNode SXmlNode::GetChildren()
{

	SXmlNode node;
	if (_node){
	//	node._node= _node->children;
		xmlNodePtr tmp_node =_node->children;
		if ((tmp_node!=NULL)&&(tmp_node->type == XML_TEXT_NODE)){
			tmp_node = tmp_node->next;
		}
		node._node = tmp_node;
	}else {
		node._node=NULL;
	}
	return node;
}
SXmlNode SXmlNode::GetNext()
{
	SXmlNode node;
	if (_node){
		xmlNodePtr tmp_node =_node->next;
		while((tmp_node!=NULL)&&(tmp_node->type == XML_TEXT_NODE)){
			tmp_node = tmp_node->next;	
		}
		node._node= tmp_node;
	}else {
		node._node=NULL;
	}
	return node;
}

SXmlNode SXmlNode::GetCopy()
{
	SXmlNode node;
	node._node = xmlCopyNode(_node,1);
	return node;
}
SXmlNode SXmlNode::AddChild(const SXmlNode& node)
{
	if (_node){
		xmlNodePtr child=xmlAddChild(_node,node._node);
		SXmlNode cnode;
		cnode._node=child;
		return cnode;
	}else {

		SXmlNode cnode;
		cnode._node=NULL;
		return cnode;
	}
}

SXmlNode SXmlNode::NewChild(const std::string& name,const std::string& content)
{
	if (!_node){
		SXmlNode node;
		node._node=NULL;
		return node;
	}
	xmlNodePtr nodeptr=xmlNewChild(_node,NULL,(const xmlChar*)name.c_str(),(const xmlChar *)content.c_str());
	SXmlNode node;
	if (nodeptr){
		node._node=nodeptr;
	}else {
		node._node=NULL;
	}
	return node;
}

SXmlNode SXmlNode::GetChildren(const std::string& name)
{
	SXmlNode node = GetChildren();
	
	while(!node.Null())
	{
		if (node.GetName() == name){
			return node;
		}
		node = node.GetNext();
	}
	
	SXmlNode n;
	n._node=NULL;
	return n;
}
std::string SXmlNode::GetAttribute(std::string name)
{
	if (!_node)
		return "";
	
	xmlChar * str = xmlGetProp(_node,(const xmlChar *) (name.c_str()));
	if (str){
		std::string result((char*)str);
		xmlFree(str);
		return result;
	}else{
		return "";
	}
}
int SXmlNode::SetAttribute(std::string name,std::string value)
{
	if (!_node)
		return -1;
	xmlSetProp(_node,(const xmlChar *) (name.c_str()),(const xmlChar*)(value.c_str()));
	return 0;
}
int SXmlNode::SetContent(std::string value)
{
	if (!_node)
		return -1;
	xmlNodeSetContent(_node, BAD_CAST value.c_str());
	return 0;
}
std::string SXmlNode::GetContent()
{
	if (!_node)
		return "";
	xmlChar * str=xmlNodeGetContent(_node);
	if (str){
		std::string result((char*)str);
		xmlFree(str);
		return result;
	}else{
		return "";
	}
}

int SXmlNode::NewAttribute(std::string name,std::string value)
{
	if (!_node)
		return -1;

	xmlAttrPtr newattr;
	newattr = xmlNewProp (_node, (const xmlChar *) (name.c_str()), (const xmlChar *) (value.c_str()));
	if (newattr)
		return 0;
	else 
		return -1;
}
int SXmlNode::SetContentInCData(std::string value)
{
	if (!_node)
		return -1;
	xmlNodePtr new_node=NULL;
	xmlNodePtr new_root_node=xmlNewDocNode(NULL,0, _node->name,(const xmlChar *)"");
	new_node = xmlNewCDataBlock(NULL,(const xmlChar *) (value.c_str()), value.length());
	xmlAddChild(new_root_node,new_node);
	//xmlNodeSetName(new_node, _node->name);
	xmlReplaceNode(_node,new_root_node);
	xmlFreeNode(_node);	
	_node=new_root_node;	
	return 0;
}
void SXmlNode::SetNodeName(const std::string& tagName)
{
	if (_node)
		return xmlNodeSetName(_node, (const xmlChar*)tagName.c_str());
	else 
		return ;	
}

std::string SXmlNode::GetNodeListString(SXmlDocument& doc)
{
	if(!_node)
		return "";
	if (!doc._doc)
		return "";
	xmlChar * str = xmlNodeListGetRawString(doc._doc, _node, 1);
	if(str){
		std::string result((char*)str);
		xmlFree(str);
		return result;
	}else{
		return "";
	}
}
void SXmlNode::SetName(const std::string& name)
{
    return xmlNodeSetName(_node, (const xmlChar*)name.c_str());
}

int SXmlNode::GetType()
{
	if (_node)
	{
		return _node->type;
	}
	else
	{
		return XML_UNKNOWN;
	}
}

bool SXmlNode::HasAttribute(const std::string& name)
{
    if(!_node)
        return false;

    if(xmlHasProp(_node, (const xmlChar *)(name.c_str())))
        return true;

    return false;
}

//acquire all NOT NULL attributes of a node 
void SXmlNode::GetAttributeList(std::map<std::string, std::string>& attr_list){
    if(!_node)
	return;
    xmlAttr* attribute = _node->properties;
    while(attribute && attribute->name && attribute->children)
    {
	xmlChar* value = xmlNodeListGetString(_node->doc, attribute->children, 1);
	std::string attr_name((const char*) attribute->name);
	std::string attr_value((const char*) value);
	if(!attr_name.empty() && !attr_value.empty())
	   attr_list[attr_name] = attr_value;
	xmlFree(value); 
	attribute = attribute->next;
    } 
}

//set all NOT NULL attributes of a node 
void SXmlNode::SetAttributeList(const std::map<std::string, std::string>& attr_list){
    if(!_node)
	return;
    for(std::map<std::string, std::string>::const_iterator itr = attr_list.begin(); itr!=attr_list.end(); itr++)
	if(!(itr->first).empty() && !(itr->second).empty())
		SetAttribute(itr->first, itr->second); 
}
/*
xmlNodePtr	xmlReplaceNode		(xmlNodePtr old, 
					 xmlNodePtr cur)
					 Function: xmlReplaceNode

xmlNodePtr	xmlReplaceNode		(xmlNodePtr old, 
					 xmlNodePtr cur)

Unlink the old node from its current context, prune the new one at the same place. If @cur was already inserted in a document it is first unlinked from its existing context.
old:	the old node
cur:	the node
Returns:	the @old node
*/

