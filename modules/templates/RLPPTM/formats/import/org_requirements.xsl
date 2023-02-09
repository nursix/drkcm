<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         RLPPTM ORG Requirements - CSV Import Stylesheet

         CSV column..................Format..........Content

         Type........................string..........Organisation Type Name

         Commercial..................string..........Commercial Providers
                                                     true|false (default false)
         Verification Required.......string..........Type Verification Required
                                                     true|false (default false)
         MPAV Required...............string..........MPAV Qualification Verification Required
                                                     true|false (default true)
         ReprInfo Required...........string..........Representative Info Required
                                                     true|false (default false)

    *********************************************************************** -->

    <xsl:output method="xml"/>
    <xsl:key name="types" match="row" use="col[@field='Type']"/>

    <!-- ****************************************************************** -->
    <xsl:template match="/">
        <s3xml>
            <xsl:for-each select="//row[generate-id(.)=generate-id(key('types',
                                                                   col[@field='Type'])[1])]">
                <xsl:call-template name="OrganisationType"/>
            </xsl:for-each>
            <xsl:apply-templates select="./table/row"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template name="OrganisationType">
        <xsl:variable name="Name" select="col[@field='Type']/text()"/>
        <resource name="org_organisation_type">
            <xsl:attribute name="tuid">
                <xsl:value-of select="concat('OrgType:', $Name)"/>
            </xsl:attribute>
            <data field="name">
                <xsl:value-of select="$Name"/>
            </data>
        </resource>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">
        <resource name="org_requirements">
            <reference field="organisation_type_id" resource="org_organisation_type">
                <xsl:attribute name="tuid">
                    <xsl:value-of select="concat('OrgType:', col[@field='Type']/text())"/>
                </xsl:attribute>
            </reference>
            <data field="commercial">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="col[@field='Commercial']/text()='true'">
                            <xsl:value-of select="'true'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'false'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>
            <data field="natpersn">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="col[@field='Natural Persons']/text()='true'">
                            <xsl:value-of select="'true'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'false'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>
            <data field="verifreq">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="col[@field='Verification Required']/text()='true'">
                            <xsl:value-of select="'true'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'false'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>
            <data field="mpavreq">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="col[@field='MPAV Required']/text()='false'">
                            <xsl:value-of select="'false'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'true'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>
            <data field="rinforeq">
                <xsl:attribute name="value">
                    <xsl:choose>
                        <xsl:when test="col[@field='ReprInfo Required']/text()='true'">
                            <xsl:value-of select="'true'"/>
                        </xsl:when>
                        <xsl:otherwise>
                            <xsl:value-of select="'false'"/>
                        </xsl:otherwise>
                    </xsl:choose>
                </xsl:attribute>
            </data>
        </resource>
    </xsl:template>

    <!-- ****************************************************************** -->

</xsl:stylesheet>
