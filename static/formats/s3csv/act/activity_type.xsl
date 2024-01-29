<?xml version="1.0"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         ACT Activity Types - CSV Transformation Stylesheet

         CSV column..................Format..........Content

         Type........................string..........Activity Type (name)
         Code........................string..........Activity Code
         Obsolete....................string..........Flag to indicate that type is obsolete
                                                     true|false
         Comments....................string..........Comments

    *********************************************************************** -->
    <xsl:output method="xml"/>

    <xsl:include href="../commons.xsl"/>

    <!-- ****************************************************************** -->
    <xsl:template match="/">
        <s3xml>
            <xsl:apply-templates select="table/row"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">

        <xsl:variable name="Name" select="col[@field='Type']/text()"/>
        <xsl:variable name="Code" select="col[@field='Code']/text()"/>

        <resource name="act_activity_type">
            <!-- Name and Code -->
            <data field="name"><xsl:value-of select="$Name"/></data>
            <data field="code"><xsl:value-of select="$Code"/></data>

            <!-- Obsolete Flag -->
            <xsl:call-template name="Boolean">
                <xsl:with-param name="column">Obsolete</xsl:with-param>
                <xsl:with-param name="field">obsolete</xsl:with-param>
            </xsl:call-template>

            <!-- Comments -->
            <xsl:variable name="Comments" select="col[@field='Comments']/text()"/>
            <xsl:if test="$Comments!=''">
                <data field="comments">
                    <xsl:value-of select="$Comments"/>
                </data>
            </xsl:if>
        </resource>

    </xsl:template>

    <!-- END ************************************************************** -->

</xsl:stylesheet>
