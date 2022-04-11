package edu.virginia.sqsjson;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

public class XMLNode {

    String name;
    String text = "";

    XMLNode parent = null;
    List<XMLNode> children = new LinkedList<XMLNode>();
    private final static Logger logger = Logger.getLogger(XMLNode.class);

    public XMLNode(String name)
    {
        this.name = name;
        this.text = "";
    }

    public XMLNode addChildNode(String name)
    {
        XMLNode child = new XMLNode(name);
        child.parent = this;
        this.children.add(child);
        return(child);
    }

    public XMLNode addChildNode(String name, String value)
    {
        XMLNode child = new XMLNode(name);
        child.parent = this;
        this.children.add(child);
        child.setText(value);
        return(child);
    }

    public XMLNode setText(String text)
    {
        this.text = text;
        return(this);
    }

    public void traverse(PrintWriter out) 
    {
        traverse(out, 1);
    }

    public final static String spaces = "                                ";  
    public void traverse(PrintWriter out, int level) 
    {
        String indent = spaces.substring(0, level*4);
        if (this.children.size() == 0)
        {
            if (!this.text.equals("null"))
            {
                out.println(indent+"<field name=\""+this.name+"\">"+XMLEncode(this.text)+"</field>");
            }
        }
        else
        {
            out.println(indent+"<"+this.name+">");
            for (XMLNode child : this.children)
            {
                child.traverse(out, level+1);
            }
            out.println(indent+"</"+this.name+">");
        }
    }

    private String XMLEncode(String text)
    {
        String result = text;
        result = text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">",  "&gt;")
        		     .replaceAll("&amp;(#[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f];)", "&$1");

        String legalResult = result.replaceAll("[\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u000B\u000C\u000E\u000F]", "")
        		                   .replaceAll("[\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F]", "");
        if (!result.contentEquals(legalResult))
        {
        	logger.warn("Encountered illegal character(s) in input file in range 0x00 to 0x1F, deleting it (or them)");
        }
        return result;
    }
}
