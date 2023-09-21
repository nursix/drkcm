<?xml version="1.0"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         DVR Case Flag - CSV Import Stylesheet

         CSV column...........Format..........Content

         Organisation.........string..........Organisation Name
         Branch...............string..........Organisation Branch Name (optional)
         ...SubBranch,SubSubBranch...etc (indefinite depth, must specify all from root)

         Name.................string..........Type Name
         External.............string..........Flag indicates that person is
                                              currently external
                                              true|false
         Not Transferable.....string..........Cases with this flag are not transferable
                                              true|false
         Comments.............string..........Comments

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
                    <xsl:with-param name="rows" select="//table/row"/>
                </xsl:call-template>
            </xsl:for-each>

            <xsl:apply-templates select="./table/row"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">
        <resource name="dvr_case_flag">

            <!-- Link to Organisation -->
            <reference field="organisation_id" resource="org_organisation">
                <xsl:attribute name="tuid">
                    <xsl:call-template name="OrganisationID"/>
                </xsl:attribute>
            </reference>

            <data field="name">
                <xsl:value-of select="col[@field='Name']"/>
            </data>

            <xsl:variable name="is_external" select="col[@field='External']/text()"/>
            <data field="is_external">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="$is_external='true'">
                            <xsl:value-of select="'true'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'false'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>

            <xsl:variable name="is_not_transferable" select="col[@field='Not Transferable']/text()"/>
            <data field="is_not_transferable">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="$is_not_transferable='true'">
                            <xsl:value-of select="'true'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'false'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>

            <data field="comments">
                <xsl:value-of select="col[@field='Comments']"/>
            </data>

        </resource>
    </xsl:template>

    <!-- ****************************************************************** -->

</xsl:stylesheet>
