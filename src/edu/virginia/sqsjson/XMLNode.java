package edu.virginia.sqsjson;

import org.apache.log4j.Logger;

import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
        if (text.contains("coordinates"))
        {
            result = fixCoordinates(text);
        }
        result = result.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">",  "&gt;")
                       .replaceAll("&amp;(#[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f];)", "&$1");

        String legalResult = result.replaceAll("[\u0001\u0002\u0003\u0004\u0005\u0006\u0007\u0008\u000B\u000C\u000E\u000F]", "")
                                   .replaceAll("[\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018\u0019\u001A\u001B\u001C\u001D\u001E\u001F]", "");
        if (!result.equals(legalResult))
        {
            logger.warn("Encountered illegal character(s) in input file in range 0x00 to 0x1F, deleting it (or them)");
        }
        return legalResult;
    }

    private String fixCoordinates(String text2)
    {
        Pattern coordinatePattern = Pattern.compile("(\\[[-]?)(\\d*)(\\d\\d\\d\\d)([.](\\d+))?,[ ]*([-]?)(\\d*)(\\d\\d\\d\\d)([.](\\d+))?(])");
        Matcher coordinateMatcher;
        int numFixed = 0;
        int start = 0;
        for (coordinateMatcher = coordinatePattern.matcher(text2); coordinateMatcher.find(start); coordinateMatcher = coordinatePattern.matcher(text2))
        {
            numFixed ++;
            String replaceStr = coordinateMatcher.group(1) + (coordinateMatcher.group(2).isEmpty() ? "0" : coordinateMatcher.group(2)) + "." +
            		            coordinateMatcher.group(3) + (coordinateMatcher.group(5) == null ? "" : coordinateMatcher.group(5)) + "," + 
            		            coordinateMatcher.group(6) + (coordinateMatcher.group(7).isEmpty() ? "0" : coordinateMatcher.group(7)) + "." +
            		            coordinateMatcher.group(8) + (coordinateMatcher.group(10) == null ? "" : coordinateMatcher.group(10)) + "]";
            text2 = coordinateMatcher.replaceFirst(replaceStr);
        }
        if (numFixed > 0)
        {
        	logger.warn("Encountered and fixed "+numFixed+" instances where the GeoJSON coordinates were 10000 times too large");
        }
        return(text2);
    }
}
