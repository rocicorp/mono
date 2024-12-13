import React from 'react';
import MarkdownBase from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type {Plugin} from 'unified'; // Type-only import
import {visit} from 'unist-util-visit';
import type {Element} from 'hast';

/**
 * Custom rehype plugin to transform <img> with video extensions to <video>.
 */
const rehypeImageToVideo: Plugin = () => {
  return tree => {
    visit(
      tree,
      'element',
      (node: Element, index: number | null, parent: Parent | null) => {
        // Skip already transformed nodes
        if (node.properties?.['data-transformed']) {
          return;
        }

        if (
          node.tagName === 'img' &&
          /\.(mp4|webm|ogg)$/.test(node.properties?.src as string)
        ) {
          const properties = node.properties || {};
          const title = properties.title as string | undefined;

          let poster: string | undefined;
          let width: string | undefined;
          let height: string | undefined;

          // Parse custom attributes from the title
          if (title) {
            const matches = title.match(/data-([\w-]+)=["']?([^"'\s]+)["']?/g);
            if (matches) {
              matches.forEach(attr => {
                const [key, value] = attr.split('=').map(s => s.trim());
                const cleanValue = value.replace(/['"]/g, ''); // Remove quotes
                if (key === 'data-poster') {
                  poster = cleanValue;
                } else if (key === 'data-width') {
                  width = cleanValue;
                } else if (key === 'data-height') {
                  height = cleanValue;
                }
              });
            }
          }

          if (!width || !height) {
            console.warn('Missing width or height in node:', properties);
          }

          // Transform <img> into <div> with a nested <video>
          const videoContainer = {
            type: 'element',
            tagName: 'div',
            properties: {
              'className': 'video-container',
              'data-transformed': true,
            },
            children: [
              {
                type: 'element',
                tagName: 'div',
                properties: {
                  className: 'video-wrapper',
                  style: `--video-width: ${width || '640'}; --video-height: ${
                    height || '360'
                  };`,
                },
                children: [
                  {
                    type: 'element',
                    tagName: 'video',
                    properties: {
                      'controls': true,
                      'autoplay': true,
                      'loop': true,
                      'muted': true,
                      'playsinline': true,
                      'preload': 'metadata',
                      'poster': poster || undefined,
                      'className': 'inline-video',
                      'data-width': width || '640',
                      'data-height': height || '360',
                      'src': properties.src, // Add video source
                    },
                    children: [],
                  },
                ],
              },
            ],
          };

          // Replace the parent <p> with the video container, if the parent is a <p>
          if (
            parent &&
            parent.type === 'element' &&
            parent.tagName === 'p' &&
            typeof index === 'number'
          ) {
            parent.children.splice(index, 1, videoContainer);
          } else {
            // If no <p>, replace the <img> directly
            node.tagName = 'div';
            node.properties = videoContainer.properties;
            node.children = videoContainer.children;
          }
        }
      },
    );
  };
};

export default function Markdown({children}: {children: string}) {
  return (
    <MarkdownBase
      remarkPlugins={[remarkGfm]}
      rehypePlugins={[rehypeImageToVideo]}
      components={{
        p: ({children}) => {
          // Check if the paragraph contains a block-level `div.video-container`
          const containsVideoContainer = React.Children.toArray(children).some(
            child =>
              React.isValidElement(child) &&
              child.props?.className?.includes('video-container'),
          );

          // If it does, render the children directly without a <p> wrapper
          if (containsVideoContainer) {
            return <>{children}</>;
          }

          // Otherwise, render as a normal paragraph
          return <p>{children}</p>;
        },
        // Ensure no additional processing for <img> elements
        img: ({node, ...props}) => <img {...props} />,
      }}
    >
      {children}
    </MarkdownBase>
  );
}
