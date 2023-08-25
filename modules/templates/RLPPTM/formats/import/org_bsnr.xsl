<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="1.0">

    <!-- **********************************************************************
         RLPPTM ORG BSNR - CSV Import Stylesheet

         CSV column..................Format..........Content

         ORG_ID......................string..........the OrgID tag
         BSNR........................string..........the BSNR

    *********************************************************************** -->

    <xsl:output method="xml"/>

    <!-- ****************************************************************** -->
    <xsl:template match="/">
        <s3xml>
            <xsl:apply-templates select="./table/row"/>
        </s3xml>
    </xsl:template>

    <!-- ****************************************************************** -->
    <xsl:template match="row">

        <xsl:variable name="OrgID" select="normalize-space(col[@field='ORG_ID']/text())"/>
        <xsl:variable name="BSNR" select="normalize-space(col[@field='BSNR']/text())"/>

        <xsl:if test="$OrgID!='' and $BSNR!=''">
            <resource name="org_bsnr">
                <reference field="organisation_id" resource="org_organisation">
                    <!-- Pseudo-attribute to be resolved by import_prep -->
                    <xsl:attribute name="org_id">
                        <xsl:value-of select="$OrgID"/>
                    </xsl:attribute>
                </reference>
                <data field="bsnr"><xsl:value-of select="$BSNR"/></data>
            </resource>
        </xsl:if>

    </xsl:template>

    <!-- ****************************************************************** -->

</xsl:stylesheet>
