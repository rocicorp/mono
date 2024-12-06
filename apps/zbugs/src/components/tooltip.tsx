import {
  arrow,
  autoUpdate,
  flip,
  FloatingArrow,
  FloatingPortal,
  offset,
  shift,
  useDelayGroup,
  useDismiss,
  useFloating,
  useFocus,
  useHover,
  useInteractions,
  useMergeRefs,
  useRole,
  useTransitionStatus,
} from '@floating-ui/react';
import {createContext, forwardRef, useContext, useRef, useState} from 'react';
import './tooltip.css';

type ContextType = ReturnType<typeof useTooltip> | null;

const TooltipContext = createContext<ContextType>(null);

const useTooltipContext = () => {
  const context = useContext(TooltipContext);
  if (context == null) {
    throw new Error('Tooltip components must be wrapped in <Tooltip />');
  }
  return context;
};

function useTooltip() {
  const padding = 10;
  const [isOpen, setIsOpen] = useState(false);
  const arrowRef = useRef<SVGSVGElement>(null);
  const {refs, floatingStyles, context, placement} = useFloating({
    open: isOpen,
    onOpenChange: setIsOpen,
    placement: 'top',
    middleware: [
      offset(padding),
      flip(),
      shift(),
      arrow({
        element: arrowRef,
        padding,
      }),
    ],
    whileElementsMounted: autoUpdate,
    transform: false,
  });
  const {isMounted, status} = useTransitionStatus(context);
  const {delay} = useDelayGroup(context);
  const hover = useHover(context, {move: false, delay});
  const focus = useFocus(context);
  const dismiss = useDismiss(context);
  const role = useRole(context, {
    role: 'label',
  });

  const {getReferenceProps, getFloatingProps} = useInteractions([
    hover,
    focus,
    dismiss,
    role,
  ]);

  return {
    refs,
    getReferenceProps,
    floatingStyles,
    placement,
    isMounted,
    status,
    getFloatingProps,
    arrowRef,
    context,
  };
}

export function Tooltip({children}: {children: React.ReactNode}) {
  const context = useTooltip();
  return (
    <TooltipContext.Provider value={context}>
      {children}
    </TooltipContext.Provider>
  );
}

export const TooltipTrigger = forwardRef<
  HTMLDivElement,
  React.HTMLProps<HTMLDivElement>
>(({children, ...props}, propRef) => {
  const context = useTooltipContext();
  const ref = useMergeRefs([context.refs.setReference, propRef]);
  return (
    <div ref={ref} {...context.getReferenceProps(props)}>
      {children}
    </div>
  );
});

export const TooltipContent = forwardRef<
  HTMLDivElement,
  React.HTMLProps<HTMLDivElement>
>(({children, ...props}, propRef) => {
  const context = useTooltipContext();
  const ref = useMergeRefs([context.refs.setFloating, propRef]);
  if (!context.isMounted) {
    return null;
  }
  return (
    <FloatingPortal>
      <div
        ref={ref}
        className="tooltip-content"
        style={{
          ...context.floatingStyles,
        }}
        {...context.getFloatingProps(props)}
        data-placement={context.placement}
        data-status={context.status}
      >
        {children}
        <FloatingArrow
          className="tooltip-arrow"
          ref={context.arrowRef}
          context={context.context}
          strokeWidth={1}
        />
      </div>
    </FloatingPortal>
  );
});
