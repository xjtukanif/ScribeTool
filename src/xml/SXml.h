/**
 *    \file   ScribeServer.hpp
 *    \brief  a wrap of libxml, first write by wangwei
 *    \date   08/19/2012
 *    \author kanif (xjtukanif@gmail.com)
 */

#ifndef SXML_H
#define SXML_H
#include <libxml/parser.h>
#include <libxml/xmlreader.h>
#include <string>
#include <map> 

int utf16_to_utf8(const std::string &src, std::string &result);
int utf8_to_utf16(const std::string &src, std::string &result);
int utf16_to_gbk(const std::string &src, std::string &result);
int gbk_to_utf16(const std::string &src, std::string &result);
int utf8_to_gbk(const std::string &src, std::string &result);
int gbk_to_utf8(const std::string &src, std::string &result);

class SXmlNode;

bool getXmlNodeContent(SXmlNode &node, const std::string &key, bool default_value);
int getXmlNodeContent(SXmlNode &node, const std::string &key, int default_value);
long getXmlNodeContent(SXmlNode &node, const std::string &key, long default_value);
std::string getXmlNodeContent(SXmlNode &node, const std::string &key, const std::string &default_value);
std::string getXmlNodeContent(SXmlNode &node, const std::string &key, const char *default_value);

#define USE_LIBXML
enum {
   /* XML_ELEMENT_NODE = 1,
    XML_ATTRIBUTE_NODE = 2,
    XML_TEXT_NODE = 3,
    XML_CDATA_SECTION_NODE = 4,
    XML_ENTITY_REF_NODE = 5,
    XML_ENTITY_NODE = 6,
    XML_PI_NODE = 7,
    XML_COMMENT_NODE = 8,
    XML_DOCUMENT_NODE = 9,
    XML_DOCUMENT_TYPE_NODE = 10,
    XML_DOCUMENT_FRAG_NODE = 11,
    XML_NOTATION_NODE = 12,
    XML_HTML_DOCUMENT_NODE = 13,
    XML_DTD_NODE = 14,
    XML_ELEMENT_DECL = 15,
    XML_ATTRIBUTE_DECL = 16,
    XML_ENTITY_DECL = 17,
    XML_NAMESPACE_DECL = 18,
    XML_XINCLUDE_START = 19,
    XML_XINCLUDE_END = 20,
    XML_DOCB_DOCUMENT_NODE = 21,
    */
    XML_UNKNOWN=99,
};

class SXmlNode
{
friend class SXmlDocument;
public:
	std::string GetName();
	void SetName(const std::string& name);
	///获得该节点的子节点
	SXmlNode GetChildren();
	SXmlNode GetChildren(const std::string& name);
	std::string GetAttribute(std::string name);
	int SetAttribute(std::string name,std::string value);
	SXmlNode GetCopy();
	///获得该节点的兄弟节点
	SXmlNode GetNext();
	int SetContent(std::string);
	/**
	    获取节点里面的内容
	    @see getXmlNodeContent来获取缺省内容
	*/
	std::string GetContent();
	std::string GetNodeListString(class SXmlDocument&);
	int NewAttribute(std::string name,std::string value);
	SXmlNode AddChild(const SXmlNode& node);	
	SXmlNode NewChild(const std::string& name,const std::string& content);
	void SetNodeName(const std::string& tagName);
	int SetContentInCData(std::string value);
	int GetType();
	bool HasAttribute(const std::string& name);
	bool Null();
	void Remove();
	void GetAttributeList(std::map<std::string, std::string>& attr_list);
	void SetAttributeList(const std::map<std::string, std::string>& attr_list);
private:
#ifdef USE_LIBXML
	xmlNodePtr _node;
#endif
	
};
class SXmlDocument
{
friend class SXmlNode;
public:
	SXmlDocument(){_doc=NULL;};
	virtual ~SXmlDocument();
	int Parse(const std::string& str,std::string  encoding);
	SXmlNode GetRoot();
	int SaveToString(std::string &str);
	int SaveToStringWithEncode(std::string &str,std::string out_encode);
	//void Free();
private:
	int ChangeEncodeDeclaration(std::string in,std::string &out,std::string src,std::string des);
	std::string _encoding;
#ifdef USE_LIBXML
	xmlDocPtr _doc;
#endif
};

#endif

