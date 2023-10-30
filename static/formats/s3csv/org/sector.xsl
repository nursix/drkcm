<?xml version="1.0" encoding="utf-8"?>
<xsl:stylesheet
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         Sectors and Subsectors - CSV Import Stylesheet

         CSV Column              Type          Description
         Abrv....................string........Abbreviation (unique, required)
         Name....................string........Name (defaults to Abbreviation)

    *********************************************************************** -->
    <xsl:output method="xml"/>

    <!-- ****************************************************************** -->

    <xsl:template match="/">
        <s3xml>
            <xsl:apply-templates select="table/row[normalize-space(col[@field='SubsectorOf']/text())='']"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">
        <xsl:if test="normalize-space(col[@field='Abrv']/text())!=''">
            <xsl:call-template name="Sector"/>
        </xsl:if>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template name="Sector">
        <xsl:variable name="SectorName" select="normalize-space(col[@field='Name']/text())"/>
        <xsl:variable name="SectorAbrv" select="normalize-space(col[@field='Abrv']/text())"/>

        <resource name="org_sector">
            <data field="abrv">
                <xsl:choose>
                    <xsl:when test="$SectorAbrv!=''">
                        <xsl:value-of select="$SectorAbrv"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$SectorName"/>
                    </xsl:otherwise>
                </xsl:choose>
            </data>
            <data field="name">
                <xsl:choose>
                    <xsl:when test="$SectorName!=''">
                        <xsl:value-of select="$SectorName"/>
                    </xsl:when>
                    <xsl:otherwise>
                        <xsl:value-of select="$SectorAbrv"/>
                    </xsl:otherwise>
                </xsl:choose>
            </data>
        </resource>

    </xsl:template>

    <!-- ****************************************************************** -->

</xsl:stylesheet>
