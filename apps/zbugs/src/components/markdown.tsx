import type {Element, Parent} from 'hast';
import React, {memo} from 'react';
import MarkdownBase from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type {Plugin} from 'unified'; // Type-only import
import {visit} from 'unist-util-visit';

// Type guard to check if a node is an Element
function isElement(node: Parent | null): node is Element {
  return node !== null && node.type === 'element' && 'tagName' in node;
}

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

          const videoContainer: Element = {
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
                      'src': properties.src,
                    },
                    children: [],
                  },
                ],
              },
            ],
          };

          // Use the type guard to ensure `parent` is an Element
          if (
            parent &&
            isElement(parent) &&
            parent.tagName === 'p' &&
            typeof index === 'number'
          ) {
            parent.children.splice(index, 1, videoContainer);
          } else if (parent && isElement(parent)) {
            // If parent exists but is not <p>, replace the <img> directly
            parent.children.splice(index ?? 0, 1, videoContainer);
          } else {
            // If no valid parent, replace the node itself
            node.tagName = 'div';
            node.properties = videoContainer.properties;
            node.children = videoContainer.children;
          }
        }
      },
    );
  };
};

export const Markdown = memo(({children}: {children: string}) => {
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
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        img: ({node: _node, ...props}) => <img {...props} />,
        li: ({children, ...props}) => {
          const isTask = props.className?.includes('task-list-item');
          const nodes: React.ReactNode[] = React.Children.toArray(children);
          let checkbox: React.ReactNode = null;
          let label: React.ReactNode = null;
          const rest: React.ReactNode[] = [];
          let seenCheckbox = false;
          for (let i = 0; i < nodes.length; i++) {
            const node = nodes[i];
            if (
              !seenCheckbox &&
              React.isValidElement(node) &&
              node.type === 'input' &&
              node.props.type === 'checkbox'
            ) {
              seenCheckbox = true;
              checkbox = node;
              continue;
            } else if (label === null) {
              const isWhitespace =
                typeof node === 'string' && node.trim().length === 0;
              if (!isWhitespace) {
                label = node;
                continue;
              }
            }
            rest.push(node);
          }

          if (isTask) {
            return (
              <li className="task-list-item">
                <div className="task-line">
                  {checkbox && <>{checkbox}</>}
                  {label && <>{label}</>}
                </div>
                {rest.length > 0 && <div className="task-children">{rest}</div>}
              </li>
            );
          }

          return <li {...props}>{children}</li>;
        },
      }}
    >
      {children}
    </MarkdownBase>
  );
});
