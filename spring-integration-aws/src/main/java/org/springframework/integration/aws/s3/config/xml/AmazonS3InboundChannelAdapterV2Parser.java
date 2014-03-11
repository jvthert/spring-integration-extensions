package org.springframework.integration.aws.s3.config.xml;

import org.springframework.beans.BeanMetadataElement;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.integration.config.xml.AbstractPollingInboundChannelAdapterParser;
import org.w3c.dom.Element;

public class AmazonS3InboundChannelAdapterV2Parser extends AbstractPollingInboundChannelAdapterParser {

	@Override
	protected BeanMetadataElement parseSource(final Element element, final ParserContext parserContext) {
		return null;
	}
}
