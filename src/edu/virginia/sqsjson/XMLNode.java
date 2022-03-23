package edu.virginia.sqsjson;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

public class XMLNode {

    String name;
    String text = "";

    XMLNode parent = null;
    List<XMLNode> children = new LinkedList<XMLNode>();
    
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
        result = text.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">",  "&gt;");
        return result;
    }
}
