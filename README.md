React-designer
==============

Easy to configure, lightweight, editable vector graphics in your react components.

- Supports polygon and shape designing (with bezier curves)
- Implemented default scale, rotate, drag, and arrange actions
- Custom object types and custom panels


Examples and demonstration: 
<http://fatiherikli.github.io/react-designer>

![bezier editor](http://i.imgur.com/cqTleWB.gif)

### Component: Designer

This is the main canvas component which holds the all toolset and manages all drawing data. You could use this component to create drawing canvas.

An example with default configuration:

    import Designer, {Text, Rectangle} from 'react-designer';

    class App() {
      state = {
        objects: [
          {type: "text", x: 10, y: 20, text: "Hello!", fill: "red"},
          {type: "rect", x: 50, y: 70, fill: "red"}
        ]
      }

      render() {
        return (
          <Designer width={250} height={350}
            objectTypes={{
              'text': Text,
              'rect': Rect
            }}
            onUpdate={(objects) => this.setState({objects})}
            objects={this.state.objects} />
        )
      }
    }

The `Designer` component expects the following parameters:

| Parameter        | Default                        |        |
| :-------------    |:------------------------------- | :-----  |
| width            | 300                            | The width of document  |
| height           | 300                            | The height of document  |
| canvasWidth      | null                           | The width of canvas. Same with document if it's null.  |
| canvasHeight     | null                           | The height of canvas. Same with document if it's null. |
| objects          | []                             | Your object set.  |
| onUpdate         | []                             | Your update callback.  |
| objectTypes      |  Text, Circle, Rectangle, Path | Mapping of object types. |
| snapToGrid       |  1                             | Snaps the objects accordingly this multipier. |


Object types are pure react components which are derived from `Vector`.

### Component: Vector

You can create an object type by subclassing `Vector` component. Each object types have static `meta` object which contains `icon` and `initial`, and optionally `editor` value.

Example implementation:

    class MyRectangle extends Vector {
      static meta = {
        icon: <Icon icon={'rectangle'} size={30} />,
        initial: {
          width: 5,
          height: 5,
          strokeWidth: 0,
          fill: "yellow",
          radius: 5,
          blendMode: "normal"
        }
      };

      render() {
        let {object, index} = this.props;
        return (
          <rect style={this.getStyle()}
             {...this.getObjectAttributes()}
             rx={object.radius}
             width={object.width}
             height={object.height} />
        );
      }
    }

You can register this object type in your `Designer` instance.

    <Designer 
        objectTypes={{rectangle: MyRectangle}}
        width={500}
        height={500}
        onUpdate={...}
        objects={...}
        onUpdate={...} />

Apart from meta options, the vectors have `panels` static definition which contains the available panels of their.

Here is default panels in Vector component:

    static panels = [
        SizePanel,
        TextPanel,
        StylePanel,
        ArrangePanel
    ];

### Component: Panel

You could extend this component to create completely different panel instead of builtins. 

It's a pure React component. The component have `object` and `onUpdate` props. You could reach the current state with `object`, and change this state with `onChange` callback. Let's create a dummy panel.

    class MyPanel extends Panel {
      render() {
        let {object, onChange} = this.props;
        return (
          <PropertyGroup>
              <Columns label="Colors">
                <Column>
                  <Button onClick={() => {onChange('color', 'blue')}}>
                    Make blue
                  </Button>
                </Column>
                <Column>
                  <Button onClick={() => {onChange('color', 'yellow')}}>
                    Make Yellow
                  </Button>
                </Column>
              </Columns>
          </PropertyGroup>
        );
      }
    }

### Component: Preview

You can use `Preview` component to disable editing tool set and controllers. This component just renders the SVG output of your data. It may be useful for presenting edited or created graphic, instead of building a SVG file.

The parameters are same with Designer component, except the onUpdate callback is not necessarry.

    <Preview 
      objectTypes={{rectangle: MyRectangle}}
      objects={this.state.objects}
      height={500}
      width={500} />

### To-do

I built this project to create user designed areas in my side project. So, this was a just hobby project, there may be things missing for a svg editor. I'm open to pull requests and feedback, and I need help to maintain. 

Here is a todo list that in my mind. You could extend this list.

- Implement `Export` panel
    - Export selected object
    - Export document
- Write initial tests and setup test environment
- Add a key map to keep the ratio of objects when scaling
- Implement theme support for UI

### Contributors (You can add your name here in your pull-request)

- Fatih Erikli <fatiherikli@gmail.com>

