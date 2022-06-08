
<!-- @import "[TOC]" {cmd="toc" depthFrom=1 depthTo=6 orderedList=false} -->
<!-- code_chunk_output -->

* [2 submodules](#2-submodules)
	* [1. route optimization](#1-route-optimization)
	* [2. collision avoidance](#2-collision-avoidance)
		* [2a. traffic control](#2a-traffic-control)
		* [2b. terrain adaptation](#2b-terrain-adaptation)

<!-- /code_chunk_output -->

### 2 submodules

#### 1. route optimization

#### 2. collision avoidance

##### 2a. traffic control

use distributed GD/SGD with minibatches to randomly sample quad-tuples of points: $A_1, B_1, A_2, B_2$, each denoting a column vector

let:

$$
\begin{aligned}
M = A_1 - A_2 \\
C_1 = B_1 - A_1 \\
C_2 = B_2 - A_2
\end{aligned}
$$

and G is a 3x3 matrix representing:

$$
G = C_2 C_1 ^T - C_1 C_2 ^T
$$

to find the distance between line section $A_1 B_1$ and $A_2 B_2$ we minimize the following distance:

$$
\begin{aligned}
D^2 &= || M + t1 C_1 - t2 C_2 ||^2 \\
\dot{t1}, \dot{t2} &= \arg \min_{ \{t1, t2\} } D
\end{aligned}
$$

subject to $t_1, t_2 \in [0, 1]$

The solution of which is:

$$
\begin{cases}
  \dot{t1} &= Clip(\frac{M ^T G C_2}{C_1 ^T G C_2}, [0,1]) \\
  \dot{t2} &= Clip(\frac{M ^T G C_1}{C_1 ^T G C_2}, [0,1])
\end{cases}
$$

after which the 1st-order approximation of $D$ is reduced to:

$$
\begin{aligned}
D^2 &\approx <\dot M + \dot{t1} \dot C_1 - \dot{t2} \dot C_2, M + \dot{t1} C_1 - \dot{t2} C_2> \\
&= <P, \{ (1-\dot{t_1}) A_1 - (1-\dot{t_2}) A_2 + \dot{t_1} B_1 - \dot{t_2} B_2 \}>
\end{aligned}
$$

where $P = \dot M + \dot{t1} \dot C_1 - \dot{t2} \dot C_2$, Now defining energy $E$ representing violation of clearance between line section $A_1 B_1$ and $A_2 B_2$:

$$
\begin{aligned}
E_1 &= \max \{ D^2 - R^2, 0 \}
\end{aligned}
$$
(This is One definition, the other is $E_1 = \max^2 \{ D-R, 0 \}$ which will be compared in performance)

Finally we get $E$'s gradient w.r.t. $M, C_1, C_2$

$$
\begin{cases}
\nabla_{A_1} E_1 &= (1-\dot{t1}) P \\
\nabla_{A_2} E_1 &= (\dot{t2} - 1) P \\
\nabla_{B_1} E_1 &= \dot{t1} P \\
\nabla_{B_2} E_1 &= -\dot{t2} P
\end{cases}
$$

use $- \nabla_{(A_1, A_2, B_1, B_2)}$ to update all the points until (local) minimum is reached

##### 2b. terrain adaptation

(Much simpler and of low priority)

Given 
$E_2$ can be defined as 