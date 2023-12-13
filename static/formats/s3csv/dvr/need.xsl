<?xml version="1.0"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         DVR Need Types - CSV Import Stylesheet

         CSV column..................Format..........Content

         Organisation................string..........Organisation Name
         Branch.........................optional.....Organisation Branch Name
         ...SubBranch,SubSubBranch...etc (indefinite depth, must specify all from root)

         Type........................string..........Type Name
         Code........................string..........Type Code
         Protection..................string..........is protection need type
                                                     true|false
         Comments....................string..........Comments

    *********************************************************************** -->
    <xsl:import href="../orgh.xsl"/>

    <xsl:output method="xml"/>

    <!-- ****************************************************************** -->
    <xsl:template match="/">
        <s3xml>

            <!-- Import the organisation hierarchy -->
            <xsl:for-each select="table/row[1]">
                <xsl:call-template name="OrganisationHierarchy">
                    <xsl:with-param name="level">Organisation</xsl:with-param>
                    <xsl:with-param name="rows" select="//table/row"></xsl:with-param>
                </xsl:call-template>
            </xsl:for-each>

            <!-- Need Types -->
            <xsl:apply-templates select="table/row"/>

        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">

        <xsl:variable name="Name" select="col[@field='Type']/text()"/>
        <xsl:if test="$Name!=''">
            <resource name="dvr_need">

                <!-- Name -->
                <data field="name"><xsl:value-of select="$Name"/></data>

                <!-- Code -->
                <xsl:variable name="Code" select="col[@field='Code']/text()"/>
                <xsl:if test="$Code!=''">
                    <data field="code"><xsl:value-of select="$Code"/></data>
                </xsl:if>

                <!-- Link to Organisation -->
                <xsl:variable name="Organisation" select="col[@field='Organisation']/text()"/>
                <xsl:if test="$Organisation!=''">
                    <reference field="organisation_id" resource="org_organisation">
                        <xsl:attribute name="tuid">
                            <xsl:call-template name="OrganisationID"/>
                        </xsl:attribute>
                    </reference>
                </xsl:if>

                <!-- Is Protection Need? -->
                <xsl:call-template name="Boolean">
                    <xsl:with-param name="column">Protection</xsl:with-param>
                    <xsl:with-param name="field">protection</xsl:with-param>
                </xsl:call-template>

                <!-- Comments -->
                <xsl:variable name="Comments" select="col[@field='Comments']/text()"/>
                <xsl:if test="$Comments!=''">
                    <data field="comments">
                        <xsl:value-of select="$Comments"/>
                    </data>
                </xsl:if>
            </resource>
        </xsl:if>

    </xsl:template>

    <!-- ****************************************************************** -->
    <!-- Helper for boolean fields -->
    <xsl:template name="Boolean">

        <xsl:param name="column"/>
        <xsl:param name="field"/>

        <data>
            <xsl:attribute name="field">
                <xsl:value-of select="$field"/>
            </xsl:attribute>
            <xsl:attribute name="value">
                <xsl:choose>
                    <xsl:when test="col[@field=$column]/text()='true'">
                        <xsl:value-of select="'true'"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="'false'"/>
                    </xsl:otherwise>
                </xsl:choose>
            </xsl:attribute>
        </data>

    </xsl:template>

    <!-- END ************************************************************** -->

</xsl:stylesheet>
