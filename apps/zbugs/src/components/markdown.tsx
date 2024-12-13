import MarkdownBase from 'react-markdown';
import remarkGfm from 'remark-gfm';
import type {Plugin} from 'unified'; // Type-only import
import {visit} from 'unist-util-visit';

/**
 * Custom rehype plugin to transform <img> with video extensions to <video>.
 */
const rehypeImageToVideo: Plugin = () => {
  return tree => {
    visit(tree, 'element', (node: any) => {
      if (
        node.tagName === 'img' &&
        /\.(mp4|webm|ogg)$/.test(node.properties?.src)
      ) {
        const poster = node.properties['data-poster']; // Extract the `data-poster` attribute
        node.tagName = 'video';
        node.properties = {
          ...node.properties,
          controls: true,
          autoplay: true,
          loop: true,
          muted: true,
          playsinline: true,
          preload: 'metadata',
          poster: poster || undefined, // Add the poster if present
          className: [...(node.properties?.className || []), 'inline-video'],
        };
        delete node.properties['data-poster']; // Clean up the `data-poster` attribute
      }
    });
  };
};

export default function Markdown({children}: {children: string}) {
  return (
    <MarkdownBase
      remarkPlugins={[remarkGfm]}
      rehypePlugins={[rehypeImageToVideo]}
    >
      {children}
    </MarkdownBase>
  );
}
