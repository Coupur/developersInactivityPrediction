
function [] = creat_vending_machine() 
    %step 1
    %create vending machine

    Step 1: Create Vending Machine
    %Coke
    x1 =[0, 0, 5, 5 ]
    y1 =[0, 5, 5, 0  ]

    c1=[2.5 2.5]
    hold on
    axis('equal')
    fill(x1, y1, 't' )

    xt = [2]
    yt = [2.5]
    strt = {'Coke 1.15'};
    h = text(xt, yt, strt);
    set(h, 'HorizontalAlignment', 'center')
    fill()
    hold off


    %Dr.Pepper
    x2 =[0, 0, 5, 5 ]
    y2 =[0, 5, 5, 0  ]

    c2=[2.5 2.5]
    hold on
    axis('equal')
    fill(xt, yt, 't' )

    xt = [2]
    yt = [2.5]
    strt = {'Dr.Pepper 1.50'};
    h = text(xt, yt, strt);
    set(h, 'HorizontalAlignment', 'center')
    fill()
    hold off


    %Diet Coke
    x3 =[0, 0, 5, 5 ]
    y3 =[0, 5, 5, 0  ]

    c3=[2.5 2.5]

    hold on
    axis('equal')
    fill(xt, yt, 'y' )
    xt = [2]
    yt = [2.5]
    strt = {'Diet Coke' 1.15'};
    h = text(xt, yt, strt);
    set(h, 'HorizontalAlignment', 'center')
    fill()
    hold off

    %vend
    x4 =[0, 0, 5, 5 ]
    y4 =[0, 5, 5, 0  ]

    c4=[2.5 2.5]
    hold on
    axis('equal')
    fill(xt, yt, 'b' )
    xt = [2]
    yt = [2.5]
    strt = {'Coke 1.15'};
    h = text(xt, yt, strt);
    set(h, 'HorizontalAlignment', 'center')
    fill()
    hold off

    %Fanta
    x5 =[0, 0, 5, 5 ]
    y5 =[0, 5, 5, 0  ]

    c5=[2.5 2.5]
    hold on
    axis('equal')
    fill(xt, yt, 'p' )

    xt = [2]
    yt = [2.5]
    strt = {'Fanta 0.95'};
    h = text(xt, yt, strt);
    set(h, 'HorizontalAlignment', 'center')
    fill()
    hold off

    %Sprite
    x6 =[0, 0, 5, 5 ]
    y6 =[0, 5, 5, 0  ]

    c6=[2.5 2.5]
    hold on
    axis('equal')
    fill(xt, yt, 'g' )
    xt = [2]
    yt = [2.5]
    strt = {'Sprite 1.00'};
    h = text(xt, yt, strt);
    set(h, 'HorizontalAlignment', 'center')
    hold off

    x,y = ginput(1)

end


function [ ] = coordinates( x, y)
    D = sqrt((x1_x_2)^2 + (y_1 - y_2)^2 )

    case

end     

function [] = main()



end
