package edu.virginia.sqsjson;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.regex.Pattern;

import com.amazonaws.services.sqs.model.Message;

import edu.virginia.sqsjson.JsonParser;

/**
 * Converts JSON to XML and makes sure the resulting XML 
 * does not have invalid element names.
 */
public class JsonToXMLConverter {

    XMLNode topLevel;
    XMLNode currentNode;

    JsonParser parser;

    int parserLevel = 0;

    private static final Pattern XML_TAG =
            Pattern.compile("(?m)(?s)(?i)(?<first><(/)?)(?<nonXml>.+?)(?<last>(/)?>)");

    private static final Pattern REMOVE_ILLEGAL_CHARS = 
            Pattern.compile("(i?)([^\\s=\"'a-zA-Z0-9._-])|(xmlns=\"[^\"]*\")");

    public JsonToXMLConverter()
    {
    }

    public XMLNode parseInput(Message message) 
    {
        XMLNode result = parseInput( new StringReader(message.getBody()));
        return(result);
    }

    public XMLNode parseInput(InputStream is) 
    {
        return parseInput( new InputStreamReader(is));
    }

    public XMLNode parseInput(final Reader in)
    {
        parser = new JsonParser(JsonParser.OPT_INTERN_KEYWORDS |
                JsonParser.OPT_UNQUOTED_KEYWORDS |
                JsonParser.OPT_SINGLE_QUOTE_STRINGS);
        parser.setInput("JSONSolrInput", in, false);
        int code = parser.getEventCode();

        while (true) {
            final String mname = parser.getMemberName();

            switch (code) {
                case JsonParser.EVT_OBJECT_BEGIN:
                    if (parserLevel == 0) 
                    {
                        this.currentNode = this.topLevel = new XMLNode("doc");
                    } 
                    else 
                    {
                        if (mname.equals("_childDocuments_"))
                        {
                            this.currentNode = this.topLevel.addChildNode("doc");
                        }
                        else
                        {
                            this.currentNode = this.topLevel.addChildNode(mname);
                        }
                    }

                    parserLevel++;
                    break;
                case JsonParser.EVT_OBJECT_ENDED:
                    parserLevel--;
                    if (parserLevel == 0)
                    {
                        return topLevel;
                    }
                    else 
                    {
                        this.currentNode = this.currentNode.parent;;
                    } 

                    break;
                case JsonParser.EVT_ARRAY_BEGIN:
                    //this.currentNode = this.currentNode.parent.addChildNode(mname);

                    break;
                case JsonParser.EVT_ARRAY_ENDED:
                    //this.currentNode = this.currentNode.parent;

                    break;
                case JsonParser.EVT_OBJECT_MEMBER:
                    String value = parser.getMemberValue();
                    if (JsonParser.isQuoted(value)) 
                    {
                        value = JsonParser.stripQuotes(value);
                    }
                    if (value.contains("\u000b"))
                    {
                    	value = value.replaceAll("\\u000b", "&#0085;");
                    }

                    if (this.currentNode.parent == null)
                    {
                        this.topLevel.addChildNode(mname, value);
                    }
                    else
                    {
                        this.currentNode.addChildNode(mname, value);
                    }
                    break;
                case JsonParser.EVT_INPUT_ENDED:
                    throw new RuntimeException("Premature end of input in JSON file");
            }
            code = parser.next();
        }

        // return record;
    }

    public static void main(String[] args)
    {
        JsonToXMLConverter converter = new JsonToXMLConverter();
        System.out.println("<add>");
        for (String arg : args)
        {
            File file = new File(arg);
            try {
                FileReader fr = new FileReader(file);
                XMLNode xml = converter.parseInput(fr);
                StringWriter sw = new StringWriter();
                PrintWriter out = new PrintWriter(sw);
                xml.traverse(out);
                System.out.print(sw.getBuffer().toString());
            } 
            catch (FileNotFoundException e) 
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        System.out.println("</add>");

    }
}