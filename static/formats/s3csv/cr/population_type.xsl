<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <!-- **********************************************************************
         Population Types - CSV Import Stylesheet

         CSV column...........Format..........Content

         Code.................string..........Unique Code
         Name.................string..........Name
         Comments.............string..........Comments

    *********************************************************************** -->
    <xsl:output method="xml"/>

    <!-- ****************************************************************** -->
    <xsl:template match="/">
        <s3xml>
            <xsl:apply-templates select="table/row"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">

        <xsl:variable name="Code" select="col[@field='Code']/text()"/>
        <xsl:variable name="Name" select="col[@field='Name']/text()"/>
        <xsl:variable name="Comments" select="col[@field='Comments']/text()"/>

        <xsl:if test="$Code!='' and $Name!=''">
            <resource name="cr_population_type">
                <data field="code">
                    <xsl:value-of select="$Code"/>
                </data>
                <data field="name">
                    <xsl:value-of select="$Name"/>
                </data>
                <xsl:if test="$Comments!=''">
                    <data field="comments">
                        <xsl:value-of select="$Comments"/>
                    </data>
                </xsl:if>
            </resource>
        </xsl:if>

    </xsl:template>

    <!-- ****************************************************************** -->

</xsl:stylesheet>
